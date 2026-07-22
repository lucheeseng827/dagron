"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import SpecEditorWithPreview from "@/components/dag/SpecEditorWithPreview";
import { getRunSpec, submitRun } from "@/lib/dagron-api";
import { errMsg } from "@/lib/err";

/// "Re-run with changes" modal: loads the run's stored DAG spec, lets you tweak
/// it (with a live graph preview), then launches a brand-new run from the edited
/// spec. Launching with no edits is equivalent to a plain re-run.
export default function RerunDialog({ runId, onClose }: { runId: string; onClose: () => void }) {
  const router = useRouter();
  const [spec, setSpec] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    let live = true;
    getRunSpec(runId)
      .then((r) => live && setSpec(r.yaml))
      .catch((e) => live && setError(errMsg(e)))
      .finally(() => live && setLoading(false));
    return () => {
      live = false;
    };
  }, [runId]);

  // Close on Escape for a keyboard-friendly modal — but not mid-launch, matching
  // the Cancel button, so you can't dismiss into a surprise navigation.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && !busy && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose, busy]);

  const onLaunch = async () => {
    if (spec == null) return;
    setBusy(true);
    setError(null);
    try {
      const { run_id } = await submitRun(spec);
      onClose();
      router.push(`/runs/${run_id}`);
    } catch (e) {
      // Server is the authoritative validator (cycle/dup/unknown-dep → 400).
      setError(errMsg(e));
      setBusy(false);
    }
  };

  return (
    <div
      style={{
        position: "fixed",
        inset: 0,
        zIndex: 50,
        background: "rgba(0,0,0,0.6)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        padding: 24,
      }}
      onClick={busy ? undefined : onClose}
      role="presentation"
    >
      <div
        onClick={(e) => e.stopPropagation()}
        role="dialog"
        aria-modal="true"
        aria-label="Re-run with changes"
        style={{
          width: "min(1100px, 94vw)",
          height: "min(760px, 88vh)",
          display: "flex",
          flexDirection: "column",
          background: "var(--panel)",
          border: "1px solid var(--border)",
          borderRadius: 12,
          overflow: "hidden",
          boxShadow: "0 20px 60px rgba(0,0,0,0.5)",
        }}
      >
        <header
          style={{
            display: "flex",
            alignItems: "center",
            gap: 12,
            padding: "14px 18px",
            borderBottom: "1px solid var(--border)",
          }}
        >
          <div>
            <strong style={{ fontSize: 15 }}>Re-run with changes</strong>
            <div style={{ color: "var(--muted)", fontSize: 12, marginTop: 2 }}>
              Tweak the spec, then launch a brand-new run. Launch unchanged for a plain re-run.
            </div>
          </div>
          <div style={{ flex: 1 }} />
          <button onClick={onClose} disabled={busy} className="dy-btn">
            Cancel
          </button>
          <button
            onClick={onLaunch}
            disabled={busy || loading || spec == null}
            className="dy-btn dy-btn-primary"
          >
            {busy ? "Launching…" : "▶ Launch new run"}
          </button>
        </header>

        {error && (
          <p style={{ color: "var(--red)", margin: 0, padding: "8px 18px", fontSize: 13 }}>{error}</p>
        )}

        <div style={{ flex: 1, minHeight: 0 }}>
          {loading ? (
            <Centered muted>Loading spec…</Centered>
          ) : spec != null ? (
            <SpecEditorWithPreview value={spec} onChange={setSpec} />
          ) : (
            <Centered>Couldn&apos;t load this run&apos;s spec.</Centered>
          )}
        </div>
      </div>
    </div>
  );
}

function Centered({ children, muted }: { children: React.ReactNode; muted?: boolean }) {
  return (
    <div
      style={{
        height: "100%",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        color: muted ? "var(--muted)" : "var(--red)",
        fontSize: 13,
      }}
    >
      {children}
    </div>
  );
}
