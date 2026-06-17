"use client";

import { useEffect, useState } from "react";
import { getMetrics } from "@/lib/dagron-api";
import { statusColor } from "@/lib/adapter";
import { errMsg } from "@/lib/err";
import type { MetricsResponse, StatusCount } from "@/types/dagron";

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

export default function MetricsPage() {
  const [m, setM] = useState<MetricsResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let timer: ReturnType<typeof setInterval> | null = null;
    const stop = () => {
      if (timer) clearInterval(timer);
      timer = null;
    };
    const load = () =>
      getMetrics()
        .then((data) => {
          setM(data);
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

  return (
    <div className="dy-page">
      <div className="dy-pagehead">
        <div>
          <h1 className="dy-h1" style={{ marginBottom: 0 }}>
            Metrics
          </h1>
          <p className="dy-subtitle">Live run and task counts by status.</p>
        </div>
      </div>
      {error && <p style={{ color: "var(--red)" }}>{error}</p>}
      {m && (
        <>
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
