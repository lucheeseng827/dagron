"use client";

// Lifecycle controls for a saved workflow: its state, and its definition
// history.
//
// Both exist because the alternatives were destructive. Before this the only
// way to stop a workflow was DELETE — and `schedules.workflow_id` is ON DELETE
// CASCADE, so that silently took its cron schedules with it. And editing
// overwrote the spec in place, so the previous definition was simply gone.
//
// The UI's job here is to make "paused is not deleted" obvious enough that
// nobody reaches for Delete to mean "stop for now".

import { useCallback, useEffect, useState } from "react";
import { listWorkflowVersions, setWorkflowState } from "@/lib/dagron-api";
import { errMsg } from "@/lib/err";
import { absTime, timeAgo } from "@/lib/time";
import type { WorkflowState, WorkflowVersion } from "@/types/dagron";

const STATES: { value: WorkflowState; label: string; hint: string }[] = [
  {
    value: "active",
    label: "Active",
    hint: "Runs on its schedule and can be run by hand.",
  },
  {
    value: "paused",
    label: "Paused",
    hint: "Schedules do not fire and manual runs are refused. Reversible, and the schedules themselves are untouched.",
  },
  {
    value: "retired",
    label: "Retired",
    hint: "Same as paused, plus hidden from the default list. For workflows you are done with but want to keep.",
  },
];

export default function WorkflowLifecycle({
  id,
  state,
  version,
  onChanged,
}: {
  id: string;
  state?: WorkflowState;
  version?: number;
  onChanged: () => void;
}) {
  const [versions, setVersions] = useState<WorkflowVersion[] | null>(null);
  const [showHistory, setShowHistory] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [viewing, setViewing] = useState<WorkflowVersion | null>(null);
  // Separate from `versions === null`, which also means "not fetched yet" —
  // without its own flag, a rejected request has no way to tell "still
  // loading" apart from "failed", and the panel is stuck showing "Loading…"
  // forever alongside the error.
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState<string | null>(null);

  const loadVersions = useCallback(() => {
    setHistoryLoading(true);
    setHistoryError(null);
    listWorkflowVersions(id)
      .then(setVersions)
      .catch((e) => setHistoryError(errMsg(e)))
      .finally(() => setHistoryLoading(false));
  }, [id]);

  useEffect(() => {
    if (showHistory && versions === null && !historyLoading && !historyError) {
      loadVersions();
    }
  }, [showHistory, versions, historyLoading, historyError, loadVersions]);

  // A save elsewhere (the editor's Save/Sync-to-Git) bumps `version` and
  // writes a new history row; drop the cached list so a panel that's already
  // open re-fetches instead of showing the pre-save history.
  useEffect(() => {
    setVersions(null);
    setHistoryError(null);
  }, [version]);

  const current = state ?? "active";

  async function change(next: WorkflowState) {
    if (next === current) return;
    // Retiring is the one that hides the workflow, so it is the one worth
    // confirming. Pausing is trivially reversible and asking would just be
    // noise.
    if (
      next === "retired" &&
      !window.confirm(
        "Retire this workflow?\n\nIt stops running and drops out of the default list. " +
          "Its schedules and history are kept, and you can set it active again.",
      )
    ) {
      return;
    }
    setBusy(true);
    setError(null);
    try {
      await setWorkflowState(id, next);
      onChanged();
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="dy-card" style={{ marginTop: 12 }}>
      <div className="dy-cardhead">
        <strong>Lifecycle</strong>
        {version != null && (
          <span style={{ color: "var(--dim)", fontSize: 12 }}>version {version}</span>
        )}
      </div>

      {error && (
        <p style={{ color: "var(--red)", fontSize: 12, margin: "0 0 8px" }} role="alert">
          {error}
        </p>
      )}

      <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
        {STATES.map((s) => (
          <button
            key={s.value}
            onClick={() => change(s.value)}
            disabled={busy}
            title={s.hint}
            aria-pressed={current === s.value}
            className={`dy-pill ${current === s.value ? "dy-pill-active" : ""}`}
            style={{ cursor: current === s.value ? "default" : "pointer" }}
          >
            {s.label}
          </button>
        ))}
      </div>

      {/* Say what the current state actually does. A pill reading "Paused"
          tells you the state; it does not tell you the schedules survived,
          which is the thing someone needs to know before they reach for
          Delete instead. */}
      <p style={{ color: "var(--muted)", fontSize: 12.5, margin: "8px 0 0", maxWidth: "72ch" }}>
        {STATES.find((s) => s.value === current)?.hint}
      </p>

      <div style={{ marginTop: 12, borderTop: "1px solid var(--border)", paddingTop: 10 }}>
        <button
          onClick={() => setShowHistory((v) => !v)}
          className="dy-btn"
          style={{ fontSize: 12, padding: "5px 9px" }}
        >
          {showHistory ? "Hide history" : "Definition history"}
        </button>

        {showHistory && (
          <div style={{ marginTop: 10 }}>
            {historyError ? (
              <p className="dy-empty" style={{ color: "var(--red)" }} role="alert">
                {historyError}{" "}
                <button
                  onClick={loadVersions}
                  className="dy-btn"
                  style={{ fontSize: 12, padding: "2px 8px", marginLeft: 4 }}
                >
                  Retry
                </button>
              </p>
            ) : versions === null || historyLoading ? (
              <p className="dy-empty">Loading…</p>
            ) : versions.length === 0 ? (
              // Only reachable for workflows created before versioning existed:
              // every workflow created since records v1 at creation.
              <p className="dy-empty">
                No recorded versions. History starts at the next save.
              </p>
            ) : (
              <table className="dy-table">
                <thead>
                  <tr>
                    <th>Version</th>
                    <th>Saved</th>
                    <th>By</th>
                    <th />
                  </tr>
                </thead>
                <tbody>
                  {versions.map((v) => (
                    <tr key={v.id}>
                      <td className="mono">
                        v{v.version}
                        {v.version === version && (
                          <span style={{ color: "var(--dim)" }}> · current</span>
                        )}
                      </td>
                      <td title={absTime(v.created_at)}>{timeAgo(v.created_at)}</td>
                      <td style={{ color: "var(--muted)" }}>{v.created_by ?? "—"}</td>
                      <td style={{ textAlign: "right" }}>
                        <button
                          onClick={() => setViewing(v)}
                          className="dy-btn"
                          style={{ fontSize: 12, padding: "4px 8px" }}
                        >
                          View
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        )}
      </div>

      {/* Read-only. Restoring a version is a real action with a real question
          behind it — does it become a new version, or rewind? — and shipping a
          button before that is answered would be guessing on the user's
          behalf. Copying out of here works today. */}
      {viewing && (
        <div style={{ marginTop: 12 }}>
          <div className="dy-cardhead">
            <strong>v{viewing.version}</strong>
            <button
              onClick={() => setViewing(null)}
              className="dy-btn"
              style={{ fontSize: 12, padding: "4px 8px" }}
            >
              Close
            </button>
          </div>
          <pre
            className="mono"
            style={{
              background: "var(--bg)",
              border: "1px solid var(--border)",
              borderRadius: 6,
              padding: 12,
              fontSize: 12.5,
              maxHeight: 320,
              overflow: "auto",
              whiteSpace: "pre-wrap",
            }}
          >
            {viewing.spec}
          </pre>
        </div>
      )}
    </div>
  );
}
