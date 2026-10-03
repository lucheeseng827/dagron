"use client";

// Human-in-the-loop console: every `type: approval` gate currently parked in
// `awaiting_approval`, resolvable right here (no digging into run detail).

import { useCallback, useEffect, useState } from "react";
import Link from "next/link";
import { useToast } from "@/components/Toasts";
import { approveTask, listApprovals, rejectTask } from "@/lib/dagron-api";
import { errMsg } from "@/lib/err";
import { absTime, timeAgo } from "@/lib/time";
import type { PendingApproval } from "@/types/dagron";

export default function ApprovalsPage() {
  const toast = useToast();
  const [rows, setRows] = useState<PendingApproval[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [comments, setComments] = useState<Record<string, string>>({});

  const load = useCallback(() => {
    listApprovals()
      .then((r) => {
        setRows(r);
        setError(null);
      })
      .catch((e) => setError(errMsg(e)));
  }, []);

  useEffect(() => {
    load();
    // Gates park and resolve out-of-band (other operators, timeouts) — poll.
    const t = setInterval(load, 10_000);
    return () => clearInterval(t);
  }, [load]);

  const resolve = async (a: PendingApproval, approve: boolean) => {
    if (!approve && !confirm(`Reject "${a.task_name}"? The task fails and its dependents skip.`)) return;
    setBusy(a.task_id);
    try {
      const comment = comments[a.task_id];
      // Approve exactly the bytes this page showed: the server refuses if a bound
      // artifact has changed since.
      const digests = Object.fromEntries(
        a.show.filter((s) => s.bound && s.sha256).map((s) => [s.path, s.sha256 as string]),
      );
      if (approve) await approveTask(a.run_id, a.task_id, comment, digests);
      else await rejectTask(a.run_id, a.task_id, comment);
      toast(approve ? `Approved "${a.task_name}"` : `Rejected "${a.task_name}"`);
    } catch (e) {
      toast(errMsg(e), "error");
    } finally {
      setBusy(null);
    }
    load();
  };

  return (
    <div className="dy-page">
      <div className="dy-pagehead">
        <div>
          <h1 className="dy-h1" style={{ marginBottom: 0 }}>
            Approvals
          </h1>
          <p className="dy-subtitle">
            Runs paused on a human gate. Approving lets dependents advance; rejecting fails the gate.
          </p>
        </div>
      </div>
      {error && <p style={{ color: "var(--red)" }}>{error}</p>}

      <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
        {rows.map((a) => (
          <div key={a.task_id} className="dy-card" style={{ display: "flex", alignItems: "center", gap: 14, flexWrap: "wrap" }}>
            <span className="dy-dot" style={{ background: "#a371f7" }} />
            <div style={{ minWidth: 0 }}>
              <div style={{ fontWeight: 600 }}>
                {a.task_name}
                <span style={{ color: "var(--muted)", fontWeight: 400 }}>
                  {" "}
                  in {a.workflow_name ?? "unknown workflow"}
                </span>
              </div>
              <div style={{ fontSize: 12.5, color: "var(--dim)", marginTop: 2 }} title={a.since ? absTime(a.since) : undefined}>
                waiting {a.since ? timeAgo(a.since).replace(" ago", "") : "—"} ·{" "}
                <Link href={`/runs/detail/?id=${a.run_id}&task=${encodeURIComponent(a.task_name)}`} className="mono" style={{ color: "var(--blue)" }}>
                  run {a.run_id.slice(0, 8)}
                </Link>
              </div>
            </div>
            {(a.approvers.length > 0 || a.not_triggerer) && (
              <div style={{ flexBasis: "100%", fontSize: 12.5, color: "var(--dim)" }}>
                {a.approvers.length > 0 && (
                  <>
                    Who may decide: <span className="mono">{a.approvers.join(", ")}</span>
                  </>
                )}
                {a.approvers.length > 0 && a.not_triggerer && " · "}
                {a.not_triggerer && "the person who started the run may not approve it"}
                {!a.can_approve && a.can_reject && " — you can reject but not approve"}
                {!a.can_approve && !a.can_reject && " — you are not permitted to decide this gate"}
              </div>
            )}
            {(a.message || a.show.length > 0) && (
              <div style={{ flexBasis: "100%", display: "flex", flexDirection: "column", gap: 6, fontSize: 13 }}>
                {a.message && <div style={{ whiteSpace: "pre-wrap" }}>{a.message}</div>}
                {a.show.length > 0 && (
                  <div style={{ display: "flex", gap: 10, flexWrap: "wrap", color: "var(--dim)" }}>
                    Review before deciding:
                    {a.show.map((s) => (
                      <a
                        key={s.path}
                        href={s.url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="mono"
                        style={{ color: "var(--blue)" }}
                      >
                        {s.path}
                        {s.bound && (
                          <span style={{ color: "var(--dim)" }}>
                            {" "}
                            {s.sha256 ? `(pinned sha256:${s.sha256.slice(0, 12)})` : "(not produced yet)"}
                          </span>
                        )}
                      </a>
                    ))}
                  </div>
                )}
              </div>
            )}
            <input
              value={comments[a.task_id] ?? ""}
              onChange={(e) => setComments((c) => ({ ...c, [a.task_id]: e.target.value }))}
              placeholder="Comment (optional, recorded with your decision)"
              maxLength={2000}
              aria-label={`Comment for ${a.task_name}`}
              style={{ flex: "1 1 240px", minWidth: 0 }}
            />
            <div style={{ marginLeft: "auto", display: "flex", gap: 8 }}>
              <button
                onClick={() => resolve(a, true)}
                disabled={busy === a.task_id || !a.can_approve || a.show.some((s) => s.bound && !s.sha256)}
                title={
                  !a.can_approve
                    ? "You are not allowed to approve this gate"
                    : a.show.some((s) => s.bound && !s.sha256)
                      ? "A bound artifact does not exist yet, so there is nothing to approve"
                      : undefined
                }
                className="dy-btn dy-btn-primary"
              >
                ✓ Approve
              </button>
              <button
                onClick={() => resolve(a, false)}
                disabled={busy === a.task_id || !a.can_reject}
                title={a.can_reject ? undefined : "You are not allowed to reject this gate"}
                className="dy-btn dy-btn-danger"
              >
                ✕ Reject
              </button>
            </div>
          </div>
        ))}
        {rows.length === 0 && !error && (
          <div className="dy-card">
            <p className="dy-empty" style={{ margin: 0 }}>
              Nothing awaiting approval. Add a gate to a workflow with a <code className="mono">type: approval</code>{" "}
              task (see the snippet palette in the editor).
            </p>
          </div>
        )}
      </div>
    </div>
  );
}
