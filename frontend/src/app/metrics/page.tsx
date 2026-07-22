"use client";

import { useEffect, useMemo, useState } from "react";
import { getMetrics, getMetricsTimeseries } from "@/lib/dagron-api";
import { statusColor } from "@/lib/adapter";
import { errMsg } from "@/lib/err";
import { durationSecs } from "@/lib/time";
import type { DayBucket, MetricsResponse, StatusCount } from "@/types/dagron";

const WINDOW_DAYS = 14;

function Bars({ title, data }: { title: string; data: StatusCount[] }) {
  const total = data.reduce((s, d) => s + d.count, 0) || 1;
  return (
    <div className="dy-card" style={{ flex: 1, minWidth: 280 }}>
      <div className="dy-cardhead">
        <strong>{title}</strong>
      </div>
      {data.length === 0 && <p className="dy-empty" style={{ marginTop: 0 }}>No data.</p>}
      {data.map((d) => (
        <div key={d.status} style={{ marginBottom: 10 }}>
          <div style={{ display: "flex", justifyContent: "space-between", fontSize: 13 }}>
            <span style={{ display: "inline-flex", alignItems: "center", gap: 7, color: statusColor(d.status) }}>
              <span className="dy-dot" style={{ background: statusColor(d.status) }} />
              {d.status}
            </span>
            <span className="mono">{d.count}</span>
          </div>
          <div className="dy-bar" style={{ marginTop: 5 }}>
            <div
              style={{
                width: `${(d.count / total) * 100}%`,
                height: "100%",
                background: statusColor(d.status),
              }}
            />
          </div>
        </div>
      ))}
    </div>
  );
}

/// Fill missing days so the x-axis is continuous even when nothing ran.
function fillDays(buckets: DayBucket[], days: number): DayBucket[] {
  const byDay = new Map(buckets.map((b) => [b.day, b]));
  const out: DayBucket[] = [];
  for (let i = days - 1; i >= 0; i--) {
    const d = new Date(Date.now() - i * 86400_000);
    const key = d.toISOString().slice(0, 10);
    out.push(
      byDay.get(key) ?? {
        day: key,
        succeeded: 0,
        failed: 0,
        cancelled: 0,
        active: 0,
        avg_duration_secs: null,
        max_duration_secs: null,
      },
    );
  }
  return out;
}

const dayLabel = (day: string) => day.slice(5).replace("-", "/");

/// Stacked runs-per-day chart. Segment order keeps the neutral grey between
/// green and red (red↔green is the weakest CVD pair), with 2px gaps between
/// segments; identity is carried by the legend + per-day tooltip, not color alone.
function RunsPerDay({ data }: { data: DayBucket[] }) {
  const [hover, setHover] = useState<DayBucket | null>(null);
  const max = Math.max(...data.map((b) => b.succeeded + b.cancelled + b.failed + b.active), 1);
  const LEGEND = [
    { key: "succeeded", label: "Succeeded", color: statusColor("succeeded") },
    { key: "cancelled", label: "Cancelled", color: statusColor("cancelled") },
    { key: "failed", label: "Failed", color: statusColor("failed") },
    { key: "active", label: "Still running", color: statusColor("running") },
  ] as const;
  return (
    <div className="dy-card" style={{ flex: 2, minWidth: 380 }}>
      <div className="dy-cardhead">
        <strong>Runs per day · {WINDOW_DAYS}d</strong>
        <span style={{ fontSize: 12, color: "var(--muted)", minHeight: 16 }}>
          {hover
            ? `${hover.day}: ${hover.succeeded} ok · ${hover.failed} failed · ${hover.cancelled} cancelled${hover.active ? ` · ${hover.active} running` : ""}`
            : ""}
        </span>
      </div>
      <div style={{ display: "flex", alignItems: "flex-end", gap: 4, height: 120 }} onMouseLeave={() => setHover(null)}>
        {data.map((b) => {
          const total = b.succeeded + b.cancelled + b.failed + b.active;
          return (
            <div
              key={b.day}
              onMouseEnter={() => setHover(b)}
              title={`${b.day} — ${total} run${total === 1 ? "" : "s"}`}
              style={{ flex: 1, display: "flex", flexDirection: "column-reverse", gap: 2, height: "100%", cursor: "default" }}
            >
              {total === 0 && <div style={{ height: 2, background: "rgba(255,255,255,0.06)", borderRadius: 1 }} />}
              {b.succeeded > 0 && (
                <div style={{ height: `${(b.succeeded / max) * 100}%`, minHeight: 3, background: statusColor("succeeded"), borderRadius: 2 }} />
              )}
              {b.cancelled > 0 && (
                <div style={{ height: `${(b.cancelled / max) * 100}%`, minHeight: 3, background: statusColor("cancelled"), borderRadius: 2 }} />
              )}
              {b.failed > 0 && (
                <div style={{ height: `${(b.failed / max) * 100}%`, minHeight: 3, background: statusColor("failed"), borderRadius: 2 }} />
              )}
              {b.active > 0 && (
                <div style={{ height: `${(b.active / max) * 100}%`, minHeight: 3, background: statusColor("running"), borderRadius: 2 }} />
              )}
            </div>
          );
        })}
      </div>
      <div style={{ display: "flex", gap: 4, marginTop: 4 }}>
        {data.map((b, i) => (
          <span key={b.day} className="mono" style={{ flex: 1, textAlign: "center", fontSize: 9, color: "var(--dim)" }}>
            {i % 2 === 0 ? dayLabel(b.day) : ""}
          </span>
        ))}
      </div>
      <div style={{ display: "flex", gap: 14, marginTop: 10, flexWrap: "wrap" }}>
        {LEGEND.map((l) => (
          <span key={l.key} style={{ display: "inline-flex", alignItems: "center", gap: 6, fontSize: 12, color: "var(--muted)" }}>
            <span className="dy-dot dy-dot-sm" style={{ background: l.color }} />
            {l.label}
          </span>
        ))}
      </div>
    </div>
  );
}

/// Mean run duration per day — a single magnitude series (one hue).
function DurationTrend({ data }: { data: DayBucket[] }) {
  const [hover, setHover] = useState<DayBucket | null>(null);
  const max = Math.max(...data.map((b) => b.avg_duration_secs ?? 0), 1);
  return (
    <div className="dy-card" style={{ flex: 1, minWidth: 300 }}>
      <div className="dy-cardhead">
        <strong>Avg run duration · {WINDOW_DAYS}d</strong>
        <span style={{ fontSize: 12, color: "var(--muted)", minHeight: 16 }}>
          {hover && hover.avg_duration_secs != null
            ? `${hover.day}: avg ${durationSecs(hover.avg_duration_secs)} · max ${durationSecs(hover.max_duration_secs)}`
            : ""}
        </span>
      </div>
      <div style={{ display: "flex", alignItems: "flex-end", gap: 4, height: 120 }} onMouseLeave={() => setHover(null)}>
        {data.map((b) => (
          <div
            key={b.day}
            onMouseEnter={() => setHover(b)}
            title={b.avg_duration_secs != null ? `${b.day} — avg ${durationSecs(b.avg_duration_secs)}` : `${b.day} — no finished runs`}
            style={{ flex: 1, display: "flex", flexDirection: "column", justifyContent: "flex-end", height: "100%", cursor: "default" }}
          >
            {b.avg_duration_secs != null ? (
              <div style={{ height: `${Math.max((b.avg_duration_secs / max) * 100, 3)}%`, background: "var(--blue)", borderRadius: "3px 3px 0 0", opacity: 0.9 }} />
            ) : (
              <div style={{ height: 2, background: "rgba(255,255,255,0.06)", borderRadius: 1 }} />
            )}
          </div>
        ))}
      </div>
      <div style={{ display: "flex", gap: 4, marginTop: 4 }}>
        {data.map((b, i) => (
          <span key={b.day} className="mono" style={{ flex: 1, textAlign: "center", fontSize: 9, color: "var(--dim)" }}>
            {i % 2 === 0 ? dayLabel(b.day) : ""}
          </span>
        ))}
      </div>
    </div>
  );
}

export default function MetricsPage() {
  const [m, setM] = useState<MetricsResponse | null>(null);
  const [series, setSeries] = useState<DayBucket[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let timer: ReturnType<typeof setInterval> | null = null;
    const stop = () => {
      if (timer) clearInterval(timer);
      timer = null;
    };
    const load = () =>
      Promise.all([getMetrics(), getMetricsTimeseries(WINDOW_DAYS)])
        .then(([data, ts]) => {
          setM(data);
          setSeries(ts);
          setError(null);
        })
        .catch((e) => {
          setError(errMsg(e));
          stop();
        });
    load();
    timer = setInterval(load, 5000);
    return stop;
  }, []);

  const days = useMemo(() => fillDays(series, WINDOW_DAYS), [series]);

  return (
    <div className="dy-page">
      <div className="dy-pagehead">
        <div>
          <h1 className="dy-h1" style={{ marginBottom: 0 }}>
            Metrics
          </h1>
          <p className="dy-subtitle">Run outcomes over time, live status counts, and queue health.</p>
        </div>
      </div>
      {error && <p style={{ color: "var(--red)" }}>{error}</p>}
      {m && (
        <>
          <div style={{ display: "flex", gap: 14, flexWrap: "wrap", marginBottom: 14 }}>
            <RunsPerDay data={days} />
            <DurationTrend data={days} />
          </div>
          <div style={{ display: "flex", gap: 14, flexWrap: "wrap", marginBottom: 14 }}>
            <Bars title="Runs by status" data={m.runs_by_status} />
            <Bars title="Tasks by status" data={m.tasks_by_status} />
          </div>
          <div className="dy-kpi" style={{ maxWidth: 260 }}>
            <div className="dy-kpi-label">Dead letters</div>
            <div className="dy-kpi-value" style={{ color: m.dead_letters > 0 ? "var(--red)" : undefined }}>
              {m.dead_letters}
            </div>
          </div>
        </>
      )}
    </div>
  );
}
