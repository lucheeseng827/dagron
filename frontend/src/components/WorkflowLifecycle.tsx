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

import { useCallback, useEffect, useMemo, useState } from "react";
import SpecDiff from "@/components/SpecDiff";
import { listWorkflowVersions, setWorkflowState } from "@/lib/dagron-api";
import { diffLines, diffStat } from "@/lib/diff";
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
  /// The version being compared *to*. Null until a row is opened.
  const [head, setHead] = useState<WorkflowVersion | null>(null);
  /// The version being compared *from*, by number. Null means "the origin" —
  /// resolved against the list rather than pinned to 1, because the oldest
  /// recorded version is not necessarily v1 for a workflow that predates
  /// versioning. Diffing from the origin forward is the default because the
  /// question history is opened with is "how did this drift from what we
  /// started with"; "what changed in this one save" is the per-row stat.
  const [baseVersion, setBaseVersion] = useState<number | null>(null);
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
    // The open diff refers to rows that are about to be refetched; keeping it
    // would leave a comparison pinned to a stale copy of the spec.
    setHead(null);
  }, [version]);

  // Oldest first. The list arrives newest-first (the table still shows it that
  // way), but a history is *read* forward, and every derivation below —
  // per-save change, the origin, the base default — is "against the one
  // before".
  const chronological = useMemo(() => (versions ? [...versions].reverse() : []), [versions]);
  const origin = chronological[0] ?? null;
  const base = useMemo(
    () =>
      baseVersion == null
        ? origin
        : (versions?.find((v) => v.version === baseVersion) ?? origin),
    [baseVersion, versions, origin],
  );

  /// What each save changed, against the version before it.
  ///
  /// This is what turns the table from a list of timestamps into a changelog:
  /// for most visits `+4 −1` on the row is the whole answer, and nothing has to
  /// be opened. Computed once for the list — the specs are already in hand, so
  /// this costs no requests.
  const perSave = useMemo(() => {
    const out = new Map<number, string | null>();
    for (let i = 0; i < chronological.length; i++) {
      const prev = chronological[i - 1];
      out.set(
        chronological[i].version,
        prev ? diffStat(diffLines(prev.spec, chronological[i].spec)) : null,
      );
    }
    return out;
  }, [chronological]);

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
                    <th>Changed</th>
                    <th />
                  </tr>
                </thead>
                <tbody>
                  {versions.map((v) => {
                    const stat = perSave.get(v.version);
                    const isOrigin = origin != null && v.version === origin.version;
                    return (
                      <tr key={v.id}>
                        <td className="mono">
                          v{v.version}
                          {v.version === version && (
                            <span style={{ color: "var(--dim)" }}> · current</span>
                          )}
                          {isOrigin && <span style={{ color: "var(--dim)" }}> · origin</span>}
                        </td>
                        <td title={absTime(v.created_at)}>{timeAgo(v.created_at)}</td>
                        <td style={{ color: "var(--muted)" }}>{v.created_by ?? "—"}</td>
                        {/* Against the version before it — what this one save
                            did, which is a different question from the diff
                            below (drift from the base). */}
                        <td className="mono" style={{ fontSize: 11.5 }}>
                          {isOrigin ? (
                            <span style={{ color: "var(--dim)" }}>—</span>
                          ) : stat ? (
                            <Stat stat={stat} />
                          ) : (
                            <span style={{ color: "var(--dim)" }}>no change</span>
                          )}
                        </td>
                        <td style={{ textAlign: "right" }}>
                          <button
                            onClick={() => setHead(head?.id === v.id ? null : v)}
                            className="dy-btn"
                            style={{ fontSize: 12, padding: "4px 8px" }}
                            aria-expanded={head?.id === v.id}
                            title={`Compare v${v.version} against the base below`}
                          >
                            {head?.id === v.id ? "Hide" : "Diff"}
                          </button>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            )}
          </div>
        )}
      </div>

      {/* Read-only. Restoring a version is a real action with a real question
          behind it — does it become a new version, or rewind? — and shipping a
          button before that is answered would be guessing on the user's
          behalf. Copying out of the Full YAML toggle works today. */}
      {head && base && (
        <div style={{ marginTop: 12 }}>
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 8,
              flexWrap: "wrap",
              marginBottom: 8,
            }}
          >
            <label style={{ color: "var(--muted)", fontSize: 12 }} htmlFor="diff-base">
              Compare from
            </label>
            <select
              id="diff-base"
              value={base.version}
              onChange={(e) => setBaseVersion(Number(e.target.value))}
              style={selectStyle}
              title="The version this diff is measured against."
            >
              {chronological.map((v) => (
                <option key={v.id} value={v.version} disabled={v.version === head.version}>
                  v{v.version}
                  {origin && v.version === origin.version ? " (origin)" : ""}
                </option>
              ))}
            </select>
            {/* One click back to the common case, since the base is sticky
                across rows on purpose — walking v1→v2→v3 with a fixed base is
                the reason the selector is here rather than hardcoded. */}
            {origin && base.version !== origin.version && (
              <button
                onClick={() => setBaseVersion(null)}
                className="dy-btn"
                style={{ fontSize: 11.5, padding: "3px 8px" }}
              >
                Reset to origin
              </button>
            )}
            <span style={{ flex: 1 }} />
            <button
              onClick={() => setHead(null)}
              className="dy-btn"
              style={{ fontSize: 12, padding: "4px 8px" }}
            >
              Close
            </button>
          </div>
          {base.version === head.version ? (
            <p className="dy-empty" style={{ margin: 0 }}>
              v{head.version} is the base. Pick another version to compare it with.
            </p>
          ) : (
            <SpecDiff
              base={base.spec}
              head={head.spec}
              baseLabel={`v${base.version}`}
              headLabel={`v${head.version}`}
              identical={`v${head.version} is byte-identical to v${base.version}.`}
            />
          )}
        </div>
      )}
    </div>
  );
}

/// `+4 −1`, colored. Shared by the table's per-save column; the diff header
/// renders its own because it has the raw counts to hand.
function Stat({ stat }: { stat: string }) {
  return (
    <>
      {stat.split(" ").map((part, i) => (
        <span
          key={i}
          style={{ color: part.startsWith("+") ? "var(--green)" : "var(--red)", marginRight: 5 }}
        >
          {part}
        </span>
      ))}
    </>
  );
}

const selectStyle: React.CSSProperties = {
  padding: "3px 6px",
  background: "var(--bg)",
  color: "var(--fg)",
  border: "1px solid var(--border)",
  borderRadius: 5,
  fontSize: 12,
};
