"use client";

// Backfill console: create a paced backfill job over a schedule's date range
// and watch it drip into the cluster (fired/requested), or cancel mid-flight.
// Deep-linkable: /backfills?schedule=<id> preselects the schedule.

import { useCallback, useEffect, useState } from "react";
import { useToast } from "@/components/Toasts";
import { cancelBackfill, createBackfill, listBackfills, listSchedules } from "@/lib/dagron-api";
import { errMsg } from "@/lib/err";
import { absTime, timeAgo } from "@/lib/time";
import type { BackfillView, Schedule } from "@/types/dagron";

const STATUS_COLOR: Record<string, string> = {
  running: "var(--blue)",
  completed: "var(--green)",
  cancelled: "var(--muted)",
};

/// datetime-local wants "YYYY-MM-DDTHH:mm"; default the range to the last 24h.
function toLocalInput(d: Date): string {
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

export default function BackfillsPage() {
  const toast = useToast();
  const [jobs, setJobs] = useState<BackfillView[]>([]);
  const [schedules, setSchedules] = useState<Schedule[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  // Create form.
  const [scheduleId, setScheduleId] = useState("");
  const [from, setFrom] = useState(() => toLocalInput(new Date(Date.now() - 24 * 3600_000)));
  const [to, setTo] = useState(() => toLocalInput(new Date()));
  const [maxRuns, setMaxRuns] = useState("");

  const load = useCallback(() => {
    listBackfills()
      .then((j) => {
        setJobs(j);
        setError(null);
      })
      .catch((e) => setError(errMsg(e)));
  }, []);

  useEffect(() => {
    load();
    listSchedules()
      .then((s) => {
        setSchedules(s);
        // Preselect from ?schedule= (ScheduleDrawer deep-link).
        const want = new URLSearchParams(window.location.search).get("schedule");
        if (want && s.some((x) => x.id === want)) setScheduleId(want);
        else if (s.length > 0) setScheduleId((cur) => cur || s[0].id);
      })
      .catch(() => {});
    // Running jobs advance out-of-band (the engine paces them) — poll.
    const t = setInterval(load, 5000);
    return () => clearInterval(t);
  }, [load]);

  const onCreate = async () => {
    if (!scheduleId) return;
    setBusy(true);
    try {
      const cap = maxRuns.trim() ? Number(maxRuns) : undefined;
      await createBackfill(
        scheduleId,
        new Date(from).toISOString(),
        new Date(to).toISOString(),
        cap && Number.isFinite(cap) && cap > 0 ? Math.floor(cap) : undefined,
      );
      toast("Backfill job created — the engine paces it from here");
      load();
    } catch (e) {
      toast(errMsg(e), "error");
    } finally {
      setBusy(false);
    }
  };

  const onCancel = async (id: string) => {
    if (!confirm("Cancel this backfill? Fire-times not yet materialized stay unfilled.")) return;
    setBusy(true);
    try {
      await cancelBackfill(id);
      toast("Backfill cancelled");
      load();
    } catch (e) {
      toast(errMsg(e), "error");
    } finally {
      setBusy(false);
    }
  };

  const scheduleName = (id: string) => schedules.find((s) => s.id === id)?.workflow_name ?? id.slice(0, 8);

  return (
    <div className="dy-page">
      <div className="dy-pagehead">
        <div>
          <h1 className="dy-h1" style={{ marginBottom: 0 }}>
            Backfills
          </h1>
          <p className="dy-subtitle">
            Materialize a schedule's missed fire-times over a date range — paced, deduped, cancellable.
          </p>
        </div>
      </div>
      {error && <p style={{ color: "var(--red)" }}>{error}</p>}

      {/* create form */}
      <div className="dy-card" style={{ marginBottom: 18 }}>
        <strong>New backfill</strong>
        {schedules.length === 0 ? (
          <p className="dy-empty" style={{ marginBottom: 0 }}>
            No schedules exist yet — add one to a workflow first (its cron + timezone define the
            fire-times to backfill).
          </p>
        ) : (
          <div style={{ display: "flex", gap: 10, alignItems: "flex-end", flexWrap: "wrap", marginTop: 12 }}>
            <label style={{ fontSize: 12, color: "var(--muted)" }}>
              Schedule
              <br />
              <select value={scheduleId} onChange={(e) => setScheduleId(e.target.value)} className="dy-btn" style={{ cursor: "pointer", marginTop: 4 }}>
                {schedules.map((s) => (
                  <option key={s.id} value={s.id}>
                    {s.workflow_name} · {s.cron_expr} ({s.timezone})
                  </option>
                ))}
              </select>
            </label>
            <label style={{ fontSize: 12, color: "var(--muted)" }}>
              From
              <br />
              <input type="datetime-local" value={from} onChange={(e) => setFrom(e.target.value)} className="dy-btn mono" style={{ marginTop: 4 }} />
            </label>
            <label style={{ fontSize: 12, color: "var(--muted)" }}>
              To
              <br />
              <input type="datetime-local" value={to} onChange={(e) => setTo(e.target.value)} className="dy-btn mono" style={{ marginTop: 4 }} />
            </label>
            <label style={{ fontSize: 12, color: "var(--muted)" }}>
              Max runs (optional)
              <br />
              <input value={maxRuns} onChange={(e) => setMaxRuns(e.target.value)} placeholder="no cap" className="dy-btn mono" style={{ width: 110, marginTop: 4 }} />
            </label>
            <button onClick={onCreate} disabled={busy || !scheduleId} className="dy-btn dy-btn-primary">
              Start backfill
            </button>
          </div>
        )}
      </div>

      {/* job list */}
      <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
        {jobs.map((j) => {
          const pct = j.requested > 0 ? Math.min(100, Math.round((j.fired / j.requested) * 100)) : 0;
          const color = STATUS_COLOR[j.status] ?? "var(--muted)";
          return (
            <div key={j.id} className="dy-card">
              <div style={{ display: "flex", alignItems: "center", gap: 12, flexWrap: "wrap" }}>
                <span className="dy-dot" style={{ background: color }} />
                <strong>{scheduleName(j.schedule_id)}</strong>
                <span className="dy-pill" style={{ color }}>{j.status}</span>
                <span className="mono" style={{ fontSize: 12, color: "var(--muted)" }}>
                  {j.cron_expr} · {j.timezone}
                </span>
                <span style={{ marginLeft: "auto", fontSize: 12.5, color: "var(--dim)" }} title={absTime(j.created_at)}>
                  created {timeAgo(j.created_at)}
                </span>
                {j.status === "running" && (
                  <button onClick={() => onCancel(j.id)} disabled={busy} className="dy-btn dy-btn-danger">
                    Cancel
                  </button>
                )}
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 12, marginTop: 10 }}>
                <span className="mono" style={{ fontSize: 12.5, color: "var(--muted)", whiteSpace: "nowrap" }}>
                  {j.fired} / {j.requested} fired
                </span>
                <div className="dy-bar" style={{ flex: 1, marginTop: 0 }}>
                  <div style={{ width: `${pct}%`, height: "100%", background: color }} />
                </div>
                <span className="mono" style={{ fontSize: 12, color: "var(--dim)", whiteSpace: "nowrap" }}>
                  {new Date(j.range_from).toLocaleDateString()} → {new Date(j.range_to).toLocaleDateString()}
                </span>
              </div>
            </div>
          );
        })}
        {jobs.length === 0 && !error && (
          <div className="dy-card">
            <p className="dy-empty" style={{ margin: 0 }}>No backfill jobs yet.</p>
          </div>
        )}
      </div>
    </div>
  );
}
