"use client";

// Read-only view of an archived run's `dagron.run-archive.v1` document —
// history stays browsable after the archive GC moves a run out of the hot store.

import { use, useEffect, useState } from "react";
import Link from "next/link";
import { getArchivedRun } from "@/lib/dagron-api";
import { statusColor, statusLabel } from "@/lib/adapter";
import { errMsg } from "@/lib/err";
import { absTime, duration } from "@/lib/time";
import type { ArchivedRunDoc, TaskStatus } from "@/types/dagron";

export default function ArchivedRunPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = use(params);
  const [doc, setDoc] = useState<ArchivedRunDoc | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [openTask, setOpenTask] = useState<string | null>(null);

  useEffect(() => {
    getArchivedRun(id)
      .then(setDoc)
      .catch((e) => setError(errMsg(e)));
  }, [id]);

  const run = doc?.run;
  const tasks = doc?.tasks ?? [];

  return (
    <div className="dy-page">
      <div className="dy-pagehead">
        <div>
          <h1 className="dy-h1" style={{ marginBottom: 0 }}>
            {doc?.index?.name ?? "Archived run"}{" "}
            <span className="mono" style={{ fontSize: 15, color: "var(--muted)" }}>
              {id.slice(0, 8)}
            </span>
          </h1>
          <p className="dy-subtitle">
            Archived {doc?.index?.archived_at ? absTime(doc.index.archived_at) : "—"} · read-only
          </p>
        </div>
        <Link href="/runs" className="dy-btn">
          ← Runs
        </Link>
      </div>

      {error && (
        <div className="dy-card" style={{ borderColor: "var(--red)" }}>
          <p style={{ color: "var(--red)", margin: 0 }}>{error}</p>
          <p style={{ color: "var(--muted)", fontSize: 12.5, marginBottom: 0 }}>
            A 410 means this run was compacted into the Parquet analytics dataset and no longer has a
            per-run document.
          </p>
        </div>
      )}

      {run && (
        <>
          <div style={{ display: "flex", alignItems: "center", gap: 14, marginBottom: 16 }}>
            <span style={{ display: "inline-flex", alignItems: "center", gap: 7, color: statusColor(run.status as TaskStatus) }}>
              <span className="dy-dot" style={{ background: statusColor(run.status as TaskStatus) }} />
              {run.status}
            </span>
            {run.created_at && (
              <span className="mono" style={{ fontSize: 12.5, color: "var(--muted)" }} title={absTime(run.created_at)}>
                started {absTime(run.created_at)}
              </span>
            )}
            {run.created_at && run.finished_at && (
              <span className="mono" style={{ fontSize: 12.5, color: "var(--muted)" }}>
                took {duration(run.created_at, run.finished_at)}
              </span>
            )}
          </div>

          <div className="dy-card" style={{ padding: 0, overflow: "hidden" }}>
            <div style={{ padding: "11px 18px", borderBottom: "1px solid var(--border)", fontSize: 11, fontWeight: 600, color: "var(--dim)", textTransform: "uppercase", letterSpacing: "0.05em" }}>
              Tasks ({tasks.length})
            </div>
            {tasks.map((t) => (
              <div key={t.id} style={{ borderBottom: "1px solid var(--border)" }}>
                <button
                  onClick={() => setOpenTask((o) => (o === t.id ? null : t.id))}
                  style={{ display: "flex", alignItems: "center", gap: 12, width: "100%", padding: "12px 18px", background: "none", border: "none", color: "var(--fg)", font: "inherit", cursor: "pointer", textAlign: "left" }}
                >
                  <span className="dy-dot" style={{ background: statusColor(t.status as TaskStatus) }} />
                  <span style={{ fontWeight: 600 }}>{t.name}</span>
                  <span style={{ color: statusColor(t.status as TaskStatus), fontSize: 12.5 }}>
                    {statusLabel(t.status as TaskStatus)}
                  </span>
                  {t.attempt != null && t.attempt > 1 && (
                    <span style={{ color: "var(--muted)", fontSize: 12 }}>try {t.attempt}</span>
                  )}
                  <span className="mono" style={{ marginLeft: "auto", fontSize: 12, color: "var(--muted)" }}>
                    {t.scheduled_at && t.finished_at ? duration(t.scheduled_at, t.finished_at) : ""}
                  </span>
                  <span style={{ color: "var(--dim)" }}>{openTask === t.id ? "▾" : "▸"}</span>
                </button>
                {openTask === t.id && (
                  <pre style={{ margin: 0, padding: "0 18px 14px 40px", fontSize: 12, whiteSpace: "pre-wrap", wordBreak: "break-word", color: "var(--muted)" }}>
                    {t.output || "(no output)"}
                  </pre>
                )}
              </div>
            ))}
            {tasks.length === 0 && <p className="dy-empty" style={{ padding: 16 }}>No task records in the archive document.</p>}
          </div>
        </>
      )}
    </div>
  );
}
