"use client";

// Live-updates pill: connection dot + Live/Paused switch.
//
// One control, three jobs: show whether the page is streaming ("live" green /
// "reconnecting" amber / "paused"-"offline" gray), flip the global preference,
// and — when paused — offer a manual refresh so stale pages are one click from
// current. The preference is global (lib/live.ts), so pausing here pauses
// every page.

import { connColor, useLiveUpdates, type ConnStatus } from "@/lib/live";

export default function LiveToggle({
  status,
  onRefresh,
}: {
  status: ConnStatus;
  /// Shown as a ⟳ button while paused; omit to hide.
  onRefresh?: () => void;
}) {
  const [live, setLive] = useLiveUpdates();
  const color = connColor(status);

  return (
    <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
      {!live && onRefresh && (
        <button
          type="button"
          className="dy-iconbtn"
          title="Refresh now"
          aria-label="Refresh now"
          onClick={onRefresh}
        >
          ⟳
        </button>
      )}
      <button
        type="button"
        onClick={() => setLive(!live)}
        className={`dy-pill ${live ? "dy-pill-active" : ""}`}
        style={{ cursor: "pointer", display: "inline-flex", alignItems: "center", gap: 7 }}
        title={
          live
            ? "Live updates on — pause to stop background reads"
            : "Live updates off — no background reads; resume for realtime"
        }
        aria-pressed={live}
      >
        <span className="dy-dot dy-dot-sm" style={{ background: color }} />
        {live ? statusLabel(status) : "Paused"}
      </button>
    </span>
  );
}

function statusLabel(s: ConnStatus): string {
  if (s === "reconnecting") return "Reconnecting";
  if (s === "offline") return "Offline";
  return "Live";
}
