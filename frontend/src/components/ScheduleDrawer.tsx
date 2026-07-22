"use client";

import { useCallback, useEffect, useState } from "react";
import Link from "next/link";
import {
  createSchedule,
  deleteSchedule,
  listSchedules,
  updateSchedule,
} from "@/lib/dagron-api";
import { useToast } from "@/components/Toasts";
import { errMsg } from "@/lib/err";
import { absTime, fromNow } from "@/lib/time";
import type { Schedule } from "@/types/dagron";

// cron crate uses the 6/7-field form (sec min hour dom mon dow [year]).
const PRESETS = [
  { label: "Every minute", expr: "0 * * * * *" },
  { label: "Hourly", expr: "0 0 * * * *" },
  { label: "Daily 02:00", expr: "0 0 2 * * *" },
  { label: "Weekly (Mon 09:00)", expr: "0 0 9 * * Mon" },
];

// Common IANA zones for the dropdown; the input accepts any valid name.
const TZ_SUGGESTIONS = [
  "UTC",
  "America/New_York",
  "America/Los_Angeles",
  "Europe/London",
  "Europe/Berlin",
  "Asia/Singapore",
  "Asia/Tokyo",
  "Australia/Sydney",
];

/// Schedule drawer for a workflow: list + add/toggle/delete cron schedules,
/// with the full engine surface — IANA timezone (DST-aware), auto catch-up of
/// missed fires, per-fire `when:` gates and `stopStrategy` auto-stop.
export default function ScheduleDrawer({ workflowId }: { workflowId: string }) {
  const toast = useToast();
  const [rows, setRows] = useState<Schedule[]>([]);
  const [expr, setExpr] = useState(PRESETS[2].expr);
  const [timezone, setTimezone] = useState("UTC");
  const [catchup, setCatchup] = useState(false);
  const [whenExpr, setWhenExpr] = useState("");
  const [stopExpr, setStopExpr] = useState("");
  const [advanced, setAdvanced] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const load = useCallback(() => {
    listSchedules(workflowId)
      .then(setRows)
      .catch((e) => setError(errMsg(e)));
  }, [workflowId]);
  useEffect(() => load(), [load]);

  const onAdd = async () => {
    setBusy(true);
    setError(null);
    try {
      await createSchedule(workflowId, expr.trim(), {
        timezone: timezone.trim() || "UTC",
        catchup,
        when_expr: whenExpr.trim() || undefined,
        stop_expr: stopExpr.trim() || undefined,
      });
      toast("Schedule added");
      load();
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setBusy(false);
    }
  };

  const onToggle = async (s: Schedule) => {
    setBusy(true);
    try {
      await updateSchedule(s.id, { enabled: !s.enabled });
      toast(s.enabled ? "Schedule paused" : "Schedule resumed");
      load();
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setBusy(false);
    }
  };

  const onToggleCatchup = async (s: Schedule) => {
    setBusy(true);
    try {
      await updateSchedule(s.id, { catchup: !s.catchup });
      toast(s.catchup ? "Catch-up off" : "Catch-up on — missed fires will heal");
      load();
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setBusy(false);
    }
  };

  const onDelete = async (id: string) => {
    if (!confirm("Delete this schedule?")) return;
    setBusy(true);
    try {
      await deleteSchedule(id);
      toast("Schedule deleted");
      load();
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="dy-card" style={{ marginTop: 16 }}>
      <strong>Schedules</strong>
      {error && <p style={{ color: "var(--red)", marginTop: 8 }}>{error}</p>}

      {/* existing schedules */}
      <div style={{ marginTop: 10, display: "flex", flexDirection: "column", gap: 10 }}>
        {rows.map((s) => (
          <div key={s.id} style={{ borderBottom: "1px solid var(--border)", paddingBottom: 10 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 12, flexWrap: "wrap" }}>
              <code className="mono" style={{ minWidth: 150 }}>
                {s.cron_expr}
              </code>
              <span className="dy-pill" title="IANA timezone this cron is evaluated in (DST-aware)">
                {s.timezone}
              </span>
              <span className="dy-pill" style={{ color: s.enabled ? "var(--green)" : "var(--dim)" }}>
                {s.enabled ? "enabled" : "paused"}
              </span>
              {s.catchup && (
                <span className="dy-pill" style={{ color: "var(--blue)" }} title="Missed fires are healed automatically">
                  catch-up
                </span>
              )}
              <span style={{ color: "var(--muted)", fontSize: 12 }} title={s.next_fire_at ? absTime(s.next_fire_at) : undefined}>
                next: {s.next_fire_at ? fromNow(s.next_fire_at) : "—"}
              </span>
              <div style={{ flex: 1 }} />
              <button onClick={() => onToggle(s)} disabled={busy} className="dy-btn">
                {s.enabled ? "Pause" : "Enable"}
              </button>
              <button onClick={() => onToggleCatchup(s)} disabled={busy} className="dy-btn" title="Toggle automatic catch-up of missed fires">
                {s.catchup ? "Catch-up ✓" : "Catch-up"}
              </button>
              <Link href={`/backfills?schedule=${s.id}`} className="dy-btn" title="Materialize past fire-times over a date range">
                Backfill…
              </Link>
              <button onClick={() => onDelete(s.id)} disabled={busy} className="dy-btn dy-btn-danger">
                Delete
              </button>
            </div>
            {(s.when_expr || s.stop_expr || s.stop_reason) && (
              <div style={{ marginTop: 6, fontSize: 12, color: "var(--muted)", display: "flex", gap: 14, flexWrap: "wrap" }}>
                {s.when_expr && (
                  <span>
                    when: <code className="mono">{s.when_expr}</code>
                  </span>
                )}
                {s.stop_expr && (
                  <span>
                    stop when: <code className="mono">{s.stop_expr}</code>
                  </span>
                )}
                {s.stop_reason && (
                  <span style={{ color: "var(--amber)" }} title={s.stopped_at ? absTime(s.stopped_at) : undefined}>
                    ⚠ auto-stopped: {s.stop_reason} (re-enable to resume)
                  </span>
                )}
              </div>
            )}
          </div>
        ))}
        {rows.length === 0 && <span style={{ color: "var(--muted)" }}>No schedules.</span>}
      </div>

      {/* add new */}
      <div style={{ marginTop: 14, display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
        {PRESETS.map((p) => (
          <button key={p.expr} onClick={() => setExpr(p.expr)} className="dy-pill" style={{ cursor: "pointer" }}>
            {p.label}
          </button>
        ))}
        <input
          value={expr}
          onChange={(e) => setExpr(e.target.value)}
          className="mono"
          style={{
            background: "var(--bg)",
            color: "var(--fg)",
            border: "1px solid var(--border)",
            borderRadius: 8,
            padding: "7px 10px",
            minWidth: 200,
          }}
        />
        <input
          value={timezone}
          onChange={(e) => setTimezone(e.target.value)}
          list="dy-tz-list"
          placeholder="UTC"
          title="IANA timezone (DST-aware), e.g. America/New_York"
          className="mono"
          style={{
            background: "var(--bg)",
            color: "var(--fg)",
            border: "1px solid var(--border)",
            borderRadius: 8,
            padding: "7px 10px",
            width: 170,
          }}
        />
        <datalist id="dy-tz-list">
          {TZ_SUGGESTIONS.map((z) => (
            <option key={z} value={z} />
          ))}
        </datalist>
        <label style={{ display: "inline-flex", alignItems: "center", gap: 6, fontSize: 12.5, color: "var(--muted)" }}>
          <input type="checkbox" checked={catchup} onChange={(e) => setCatchup(e.target.checked)} />
          catch-up missed fires
        </label>
        <button onClick={onAdd} disabled={busy} className="dy-btn dy-btn-primary">
          Add schedule
        </button>
        <button onClick={() => setAdvanced((a) => !a)} className="dy-pill" style={{ cursor: "pointer" }}>
          {advanced ? "Hide advanced" : "Advanced…"}
        </button>
      </div>
      {advanced && (
        <div style={{ marginTop: 10, display: "flex", gap: 10, flexWrap: "wrap" }}>
          <label style={{ fontSize: 12, color: "var(--muted)" }}>
            Per-fire gate (<code className="mono">when</code>), e.g.{" "}
            <code className="mono">{"{{ weekday }} <= 5"}</code>
            <br />
            <input
              value={whenExpr}
              onChange={(e) => setWhenExpr(e.target.value)}
              placeholder="always fire"
              className="mono"
              style={{ background: "var(--bg)", color: "var(--fg)", border: "1px solid var(--border)", borderRadius: 8, padding: "7px 10px", minWidth: 260, marginTop: 4 }}
            />
          </label>
          <label style={{ fontSize: 12, color: "var(--muted)" }}>
            Auto-stop (<code className="mono">stopStrategy</code>), e.g.{" "}
            <code className="mono">{"{{ failed }} >= 3"}</code>
            <br />
            <input
              value={stopExpr}
              onChange={(e) => setStopExpr(e.target.value)}
              placeholder="never stop"
              className="mono"
              style={{ background: "var(--bg)", color: "var(--fg)", border: "1px solid var(--border)", borderRadius: 8, padding: "7px 10px", minWidth: 260, marginTop: 4 }}
            />
          </label>
        </div>
      )}
      <p style={{ color: "var(--dim)", fontSize: 12, marginTop: 8 }}>
        Cron form: <code className="mono">sec min hour day month weekday [year]</code>, evaluated in the
        schedule&apos;s timezone (DST-aware).
      </p>
    </div>
  );
}
