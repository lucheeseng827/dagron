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
      if (approve) await approveTask(a.run_id, a.task_id);
      else await rejectTask(a.run_id, a.task_id);
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
                <Link href={`/runs/${a.run_id}?task=${encodeURIComponent(a.task_name)}`} className="mono" style={{ color: "var(--blue)" }}>
                  run {a.run_id.slice(0, 8)}
                </Link>
              </div>
            </div>
            <div style={{ marginLeft: "auto", display: "flex", gap: 8 }}>
              <button onClick={() => resolve(a, true)} disabled={busy === a.task_id} className="dy-btn dy-btn-primary">
                ✓ Approve
              </button>
              <button onClick={() => resolve(a, false)} disabled={busy === a.task_id} className="dy-btn dy-btn-danger">
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
