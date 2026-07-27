"use client";

import { useCallback, useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import {
  discardDeadLetter,
  getDeadLetterSettings,
  listDeadLetters,
  putDeadLetterSettings,
  redriveDeadLetter,
} from "@/lib/dagron-api";
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
          <p className="dy-subtitle">
            Submissions the scheduler parked after repeated failures — payloads that never
            became runs. A run that failed is a different thing, triaged on the run itself.
          </p>
        </div>
        <DlqPolicy onError={setError} />
      </div>
      {error && <p style={{ color: "var(--red)" }}>{error}</p>}
      {loading && <p className="dy-empty">Loading…</p>}
      {/* Empty is the good case here, so it says what was checked rather than
          just "nothing". A bare "Queue is empty." reads the same whether the
          queue is healthy or the page failed to load anything. */}
      {!loading && rows.length === 0 && !error && (
        <div className="dy-card" style={{ display: "flex", gap: 12, alignItems: "flex-start" }}>
          <span className="dy-dot" style={{ background: "var(--green)", marginTop: 6 }} />
          <div>
            <strong>Nothing parked.</strong>
            <p style={{ color: "var(--muted)", margin: "2px 0 0", maxWidth: "70ch" }}>
              No submission has failed to parse, and none has exhausted its retries. Anything
              that does will appear here with its payload and the last error, ready to redrive
              once you have fixed the cause.
            </p>
          </div>
        </div>
      )}
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

/// The one dead-letter knob worth exposing: how many times ingestion retries a
/// submission before parking it.
///
/// The other knob, STREAM_DLQ_PATH, is deliberately absent. It is a filesystem
/// path on the engine's own host, and a form that chooses where a server
/// process writes files is a footgun — that one stays deployment config.
///
/// Unset means the engine keeps using its DEAD_LETTER_MAX_ATTEMPTS environment
/// value, so an existing deployment is unchanged until someone overrides it
/// here on purpose. Saving takes effect on the next ingestion failure: the
/// engine reads this on the failure path rather than caching it at startup, so
/// there is no restart and no window where this page disagrees with what is
/// actually running.
function DlqPolicy({ onError }: { onError: (m: string) => void }) {
  const [value, setValue] = useState<string>("");
  const [saved, setSaved] = useState<number | null>(null);
  const [busy, setBusy] = useState(false);
  const [loaded, setLoaded] = useState(false);

  useEffect(() => {
    getDeadLetterSettings()
      .then((s) => {
        setSaved(s?.max_attempts ?? null);
        setValue(s ? String(s.max_attempts) : "");
      })
      .catch(() => {
        /* Reading the policy is not worth an error banner over the queue
           itself; the field simply stays empty and shows the default. */
      })
      .finally(() => setLoaded(true));
  }, []);

  async function save() {
    const n = Number(value);
    if (!Number.isInteger(n) || n < 1) {
      onError("Retries before parking must be a whole number of 1 or more.");
      return;
    }
    setBusy(true);
    try {
      const next = await putDeadLetterSettings(n);
      setSaved(next.max_attempts);
    } catch (e) {
      onError(errMsg(e));
    } finally {
      setBusy(false);
    }
  }

  if (!loaded) return null;
  const dirty = value !== (saved === null ? "" : String(saved));

  return (
    // Bordered: the page body is a list of cards, and an unbordered input
    // floating in the corner reads as something that fell off rather than a
    // setting that belongs to this screen.
    <div
      className="dy-card"
      style={{
        display: "flex",
        alignItems: "flex-end",
        gap: 8,
        padding: "10px 12px",
        flexShrink: 0,
      }}
    >
      <label style={{ fontSize: 12, color: "var(--muted)" }}>
        Retries before parking
        <input
          value={value}
          onChange={(e) => setValue(e.target.value)}
          inputMode="numeric"
          // Empty is meaningful: it means "unset, use the deployment default".
          placeholder={saved === null ? "3 (default)" : undefined}
          title="How many times a failing submission is retried before it is parked here. Applies to the next ingestion failure — no restart."
          style={{
            display: "block",
            width: 120,
            marginTop: 4,
            background: "var(--bg)",
            color: "var(--fg)",
            border: `1px solid ${dirty ? "var(--accent)" : "var(--border)"}`,
            borderRadius: 6,
            padding: "6px 9px",
            fontSize: 13,
          }}
        />
      </label>
      <button className="dy-btn" onClick={save} disabled={busy || !dirty}>
        {busy ? "Saving…" : "Save"}
      </button>
    </div>
  );
}
