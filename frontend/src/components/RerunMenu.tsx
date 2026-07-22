"use client";

import { useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { resubmitRun } from "@/lib/dagron-api";
import { errMsg } from "@/lib/err";

/// A single "Re-run" control that offers both paths from one button: a quick
/// re-run of the unchanged spec, or "with changes" (which opens the editor via
/// `onEdit`). Keeps the header to one button instead of two.
export default function RerunMenu({
  runId,
  disabled,
  onError,
  onEdit,
}: {
  runId: string;
  disabled?: boolean;
  onError: (msg: string) => void;
  onEdit: () => void;
}) {
  const router = useRouter();
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  // Close the menu on outside click or Escape.
  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && setOpen(false);
    window.addEventListener("mousedown", onDown);
    window.addEventListener("keydown", onKey);
    return () => {
      window.removeEventListener("mousedown", onDown);
      window.removeEventListener("keydown", onKey);
    };
  }, [open]);

  const onResubmit = async () => {
    setOpen(false);
    setBusy(true);
    try {
      const { run_id } = await resubmitRun(runId);
      router.push(`/runs/${run_id}`);
    } catch (e) {
      onError(errMsg(e));
      setBusy(false);
    }
  };

  return (
    <div ref={ref} style={{ position: "relative" }}>
      <button
        onClick={() => setOpen((o) => !o)}
        disabled={disabled || busy}
        className="dy-btn"
        aria-haspopup="menu"
        aria-expanded={open}
        title="Re-run this workflow"
      >
        {busy ? "Re-running…" : "⟳ Re-run ▾"}
      </button>
      {open && (
        <div
          role="menu"
          style={{
            position: "absolute",
            top: "calc(100% + 6px)",
            right: 0,
            zIndex: 30,
            minWidth: 230,
            background: "var(--panel)",
            border: "1px solid var(--border)",
            borderRadius: 8,
            boxShadow: "0 10px 30px rgba(0,0,0,0.4)",
            padding: 5,
            display: "flex",
            flexDirection: "column",
            gap: 2,
          }}
        >
          <MenuItem
            label="Re-run unchanged"
            sub="Same spec, brand-new run"
            title="Start a fresh run from this run's stored definition"
            onClick={onResubmit}
          />
          <MenuItem
            label="Re-run with changes…"
            sub="Edit the spec, preview, then launch"
            title="Open the spec, tweak parameters, then launch a new run"
            onClick={() => {
              setOpen(false);
              onEdit();
            }}
          />
        </div>
      )}
    </div>
  );
}

function MenuItem({
  label,
  sub,
  title,
  onClick,
}: {
  label: string;
  sub: string;
  title: string;
  onClick: () => void;
}) {
  const [hover, setHover] = useState(false);
  return (
    <button
      role="menuitem"
      title={title}
      onClick={onClick}
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      style={{
        textAlign: "left",
        background: hover ? "var(--panel-2)" : "transparent",
        border: "none",
        borderRadius: 6,
        padding: "8px 10px",
        cursor: "pointer",
        color: "var(--fg)",
        display: "flex",
        flexDirection: "column",
        gap: 2,
      }}
    >
      <span style={{ fontSize: 13, fontWeight: 600 }}>{label}</span>
      <span style={{ fontSize: 11, color: "var(--muted)" }}>{sub}</span>
    </button>
  );
}
