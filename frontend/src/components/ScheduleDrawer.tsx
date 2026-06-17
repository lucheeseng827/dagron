"use client";

import { useCallback, useEffect, useState } from "react";
import {
  createSchedule,
  deleteSchedule,
  listSchedules,
  updateSchedule,
} from "@/lib/dagron-api";
import { errMsg } from "@/lib/err";
import type { Schedule } from "@/types/dagron";

// cron crate uses the 6/7-field form (sec min hour dom mon dow [year]).
const PRESETS = [
  { label: "Every minute", expr: "0 * * * * *" },
  { label: "Hourly", expr: "0 0 * * * *" },
  { label: "Daily 02:00", expr: "0 0 2 * * *" },
  { label: "Weekly (Mon 09:00)", expr: "0 0 9 * * Mon" },
];

/// Schedule drawer for a workflow: list + add/toggle/delete cron schedules.
/// The engine fires them (leadership-gated); this only manages the rows.
export default function ScheduleDrawer({ workflowId }: { workflowId: string }) {
  const [rows, setRows] = useState<Schedule[]>([]);
  const [expr, setExpr] = useState(PRESETS[2].expr);
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
      await createSchedule(workflowId, expr.trim());
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
      load();
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setBusy(false);
    }
  };

  const onDelete = async (id: string) => {
    setBusy(true);
    try {
      await deleteSchedule(id);
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
      <div style={{ marginTop: 10, display: "flex", flexDirection: "column", gap: 8 }}>
        {rows.map((s) => (
          <div key={s.id} style={{ display: "flex", alignItems: "center", gap: 12 }}>
            <code className="mono" style={{ minWidth: 160 }}>
              {s.cron_expr}
            </code>
            <span className="dy-pill" style={{ color: s.enabled ? "var(--green)" : "var(--dim)" }}>
              {s.enabled ? "enabled" : "paused"}
            </span>
            <span style={{ color: "var(--muted)", fontSize: 12 }}>
              next: {s.next_fire_at ?? "—"}
            </span>
            <div style={{ flex: 1 }} />
            <button onClick={() => onToggle(s)} disabled={busy} className="dy-btn">
              {s.enabled ? "Pause" : "Enable"}
            </button>
            <button onClick={() => onDelete(s.id)} disabled={busy} className="dy-btn dy-btn-danger">
              Delete
            </button>
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
            minWidth: 220,
          }}
        />
        <button onClick={onAdd} disabled={busy} className="dy-btn dy-btn-primary">
          Add schedule
        </button>
      </div>
      <p style={{ color: "var(--dim)", fontSize: 12, marginTop: 8 }}>
        Cron form: <code className="mono">sec min hour day month weekday [year]</code>
      </p>
    </div>
  );
}
