"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import Link from "next/link";
import { listDatasetEvents, listDatasets } from "@/lib/dagron-api";
import { errMsg } from "@/lib/err";
import { absTime, timeAgo } from "@/lib/time";
import type { Dataset, DatasetEvent } from "@/types/dagron";

/// Data-aware scheduling, made visible: the dataset registry plus the lineage
/// ledger behind it. The point of `produces:` is the cross-workflow update trail
/// — who refreshed what, when, and which workflows woke up because of it — and
/// until now that trail was reachable only by curling the engine's ops API.
///
/// Read-only: a dataset is updated by a task declaring `produces:`, never here.
export default function DatasetsPage() {
  const [rows, setRows] = useState<Dataset[]>([]);
  const [events, setEvents] = useState<DatasetEvent[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  // Guards onSelect against out-of-order lineage responses (last click wins).
  const requestRef = useRef(0);

  const load = useCallback(() => {
    setLoading(true);
    setError(null);
    Promise.all([listDatasets(), listDatasetEvents(undefined, 100)])
      .then(([d, e]) => {
        setRows(d);
        setEvents(e);
      })
      .catch((e) => setError(errMsg(e)))
      .finally(() => setLoading(false));
  }, []);
  useEffect(() => load(), [load]);

  // Selecting a dataset scopes the trail to it; the full ledger is the default.
  const onSelect = (uri: string) => {
    const next = selected === uri ? null : uri;
    setSelected(next);
    const id = ++requestRef.current;
    listDatasetEvents(next ?? undefined, 100)
      .then((e) => {
        if (id === requestRef.current) setEvents(e);
      })
      .catch((e) => {
        if (id === requestRef.current) setError(errMsg(e));
      });
  };

  return (
    <div className="dy-page">
      <div className="dy-pagehead">
        <div>
          <h1 className="dy-h1" style={{ marginBottom: 0 }}>
            Datasets
          </h1>
          <p className="dy-subtitle">
            What each workflow produces, when it last landed, and who wakes up when it does.
          </p>
        </div>
        <button onClick={load} className="dy-pill" style={{ cursor: "pointer" }}>
          Refresh
        </button>
      </div>

      {error && <p style={{ color: "var(--red)" }}>{error}</p>}
      {loading && <p className="dy-empty">Loading…</p>}
      {!loading && rows.length === 0 && !error && (
        <p className="dy-empty">
          No datasets yet. A task records one by declaring <code>produces: [&quot;s3://…&quot;]</code>.
        </p>
      )}

      {rows.map((d) => (
        <div
          key={d.uri}
          className="dy-card"
          style={{
            marginBottom: 12,
            borderColor: selected === d.uri ? "var(--blue)" : undefined,
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: 12, flexWrap: "wrap" }}>
            <code style={{ color: "var(--accent)", wordBreak: "break-all" }}>{d.uri}</code>
            <span className="dy-pill" title="Times this dataset has been recorded as updated">
              {d.updates} update{d.updates === 1 ? "" : "s"}
            </span>
            <span style={{ color: "var(--muted)", fontSize: 12 }} title={absTime(d.updated_at)}>
              last {timeAgo(d.updated_at)}
              {d.last_task ? ` · by ${d.last_task}` : ""}
            </span>
            <div style={{ flex: 1 }} />
            {d.last_run_id && (
              <Link href={`/runs/${d.last_run_id}`} className="dy-pill" style={{ cursor: "pointer" }}>
                Last run
              </Link>
            )}
            <button onClick={() => onSelect(d.uri)} className="dy-pill" style={{ cursor: "pointer" }}>
              {selected === d.uri ? "Clear filter" : "Lineage"}
            </button>
          </div>
          <div style={{ marginTop: "0.5rem", fontSize: 12.5, color: "var(--muted)" }}>
            {d.consumers.length > 0 ? (
              <>
                Wakes:{" "}
                {d.consumers.map((c) => (
                  <span key={c} className="dy-pill" style={{ marginRight: 6 }}>
                    {c}
                  </span>
                ))}
              </>
            ) : (
              // Not an error state: most datasets are produced for a sensor or
              // for humans, and `on_datasets:` is only one way to consume one.
              <span style={{ color: "var(--dim)" }}>No workflow subscribes to this dataset.</span>
            )}
          </div>
        </div>
      ))}

      {!loading && (
        <>
          <h2 className="dy-h2" style={{ marginTop: "1.5rem" }}>
            Lineage {selected ? <code style={{ color: "var(--accent)" }}>{selected}</code> : "(all datasets)"}
          </h2>
          {events.length === 0 ? (
            <p className="dy-empty">No updates recorded.</p>
          ) : (
            <div className="dy-card">
              <table className="dy-table" style={{ width: "100%" }}>
                <thead>
                  <tr>
                    <th style={{ textAlign: "left" }}>When</th>
                    <th style={{ textAlign: "left" }}>Dataset</th>
                    <th style={{ textAlign: "left" }}>Workflow</th>
                    <th style={{ textAlign: "left" }}>Task</th>
                    <th style={{ textAlign: "left" }}>Run</th>
                  </tr>
                </thead>
                <tbody>
                  {events.map((e) => (
                    <tr key={e.id}>
                      <td style={{ color: "var(--muted)", whiteSpace: "nowrap" }} title={absTime(e.at)}>
                        {timeAgo(e.at)}
                      </td>
                      <td className="mono" style={{ wordBreak: "break-all" }}>
                        {e.uri}
                      </td>
                      <td>{e.workflow ?? "—"}</td>
                      <td>{e.task_name ?? "—"}</td>
                      <td>
                        {e.run_id ? (
                          <Link href={`/runs/${e.run_id}`} className="mono" style={{ color: "var(--blue)" }}>
                            {e.run_id.slice(0, 8)}
                          </Link>
                        ) : (
                          // `source` is 'task' for produces:, or an external
                          // signal on builds that accept them — no run to link.
                          <span style={{ color: "var(--dim)" }}>{e.source}</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </div>
  );
}
