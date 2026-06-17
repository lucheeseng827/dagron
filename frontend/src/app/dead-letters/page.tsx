"use client";

import { useCallback, useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { discardDeadLetter, listDeadLetters, redriveDeadLetter } from "@/lib/dagron-api";
import { errMsg } from "@/lib/err";
import { timeAgo } from "@/lib/time";
import type { DeadLetter } from "@/types/dagron";

export default function DeadLettersPage() {
  const router = useRouter();
  const [rows, setRows] = useState<DeadLetter[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState<string | null>(null);
  const [open, setOpen] = useState<string | null>(null);

  const load = useCallback(() => {
    setLoading(true);
    listDeadLetters()
      .then(setRows)
      .catch((e) => setError(errMsg(e)))
      .finally(() => setLoading(false));
  }, []);
  useEffect(() => load(), [load]);

  const onRedrive = async (id: string) => {
    setBusy(id);
    setError(null);
    try {
      const { run_id } = await redriveDeadLetter(id);
      router.push(`/runs/${run_id}`);
    } catch (e) {
      setError(errMsg(e));
      setBusy(null);
      load();
    }
  };

  const onDiscard = async (id: string) => {
    if (!confirm("Discard this dead letter? This cannot be undone.")) return;
    setBusy(id);
    try {
      await discardDeadLetter(id);
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setBusy(null);
      load();
    }
  };

  return (
    <div className="dy-page">
      <div className="dy-pagehead">
        <div>
          <h1 className="dy-h1" style={{ marginBottom: 0 }}>
            Dead letters
          </h1>
          <p className="dy-subtitle">Submissions the scheduler parked after repeated failures.</p>
        </div>
      </div>
      {error && <p style={{ color: "var(--red)" }}>{error}</p>}
      {loading && <p className="dy-empty">Loading…</p>}
      {!loading && rows.length === 0 && !error && <p className="dy-empty">Queue is empty.</p>}
      {rows.map((d) => (
        <div key={d.id} className="dy-card" style={{ marginBottom: 12 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 12, flexWrap: "wrap" }}>
            <code style={{ color: "var(--accent)" }}>{d.id.slice(0, 8)}</code>
            <span className="dy-pill">{d.source}</span>
            <span style={{ color: "var(--muted)", fontSize: 12 }}>×{d.failures} · last {timeAgo(d.last_error_at)}</span>
            <div style={{ flex: 1 }} />
            <button onClick={() => setOpen(open === d.id ? null : d.id)} className="dy-pill" style={{ cursor: "pointer" }}>
              {open === d.id ? "Hide" : "Payload"}
            </button>
            <button
              onClick={() => onRedrive(d.id)}
              disabled={busy === d.id}
              className="dy-pill dy-pill-active"
              style={{ cursor: "pointer" }}
            >
              Redrive
            </button>
            <button
              onClick={() => onDiscard(d.id)}
              disabled={busy === d.id}
              className="dy-pill"
              style={{ cursor: "pointer", color: "#f85149" }}
            >
              Discard
            </button>
          </div>
          <p style={{ color: "#f85149", fontSize: 13, marginTop: "0.5rem" }}>{d.error}</p>
          {open === d.id && (
            <pre
              style={{
                background: "var(--bg)",
                padding: "0.75rem",
                borderRadius: 6,
                fontSize: 12,
                whiteSpace: "pre-wrap",
                marginTop: "0.5rem",
              }}
            >
              {d.payload}
            </pre>
          )}
        </div>
      ))}
    </div>
  );
}
