"use client";

import { useCallback, useEffect, useState } from "react";
import { connectGitRepo, disconnectGitRepo, listGitRepos, syncGitRepo } from "@/lib/dagron-api";
import { errMsg } from "@/lib/err";
import { timeAgo } from "@/lib/time";
import type { GitRepo, GitRepoState } from "@/types/dagron";

const STATE_COLOR: Record<GitRepoState, string> = {
  Synced: "#2ea043",
  OutOfSync: "#d29922",
  Syncing: "#2f81f7",
};
const STATE_TINT: Record<GitRepoState, string> = {
  Synced: "rgba(46,160,67,0.14)",
  OutOfSync: "rgba(210,153,34,0.14)",
  Syncing: "rgba(47,129,247,0.14)",
};

function syncedLabel(r: GitRepo): string {
  if (r.state === "Syncing") return "syncing now…";
  if (!r.last_synced_at) return "never synced";
  return `${r.state === "Synced" ? "synced" : "last sync"} ${timeAgo(r.last_synced_at)}`;
}

export default function GitOpsPage() {
  const [repos, setRepos] = useState<GitRepo[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState<string | null>(null);
  const [showForm, setShowForm] = useState(false);
  const [url, setUrl] = useState("");
  const [branch, setBranch] = useState("main");
  const [autoSync, setAutoSync] = useState(true);

  const load = useCallback(() => {
    listGitRepos()
      .then(setRepos)
      .catch((e) => setError(errMsg(e)))
      .finally(() => setLoading(false));
  }, []);
  useEffect(() => load(), [load]);

  const onConnect = async () => {
    if (!url.trim()) return;
    setBusy("connect");
    setError(null);
    try {
      await connectGitRepo(url.trim(), branch.trim() || "main", autoSync);
      setUrl("");
      setShowForm(false);
      load();
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setBusy(null);
    }
  };

  const onSync = async (id: string) => {
    setBusy(id);
    try {
      await syncGitRepo(id);
      load();
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setBusy(null);
    }
  };

  const onDisconnect = async (id: string) => {
    if (!confirm("Disconnect this repository?")) return;
    setBusy(id);
    try {
      await disconnectGitRepo(id);
      load();
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setBusy(null);
    }
  };

  return (
    <div className="dy-page">
      <div className="dy-pagehead">
        <div>
          <h1 className="dy-h1" style={{ marginBottom: 0 }}>
            GitOps
          </h1>
          <p className="dy-subtitle">
            Connect a repo and dagron discovers, syncs and runs its workflows — no UI edits needed.
          </p>
        </div>
        <button
          onClick={() => setShowForm((v) => !v)}
          className="dy-btn"
          style={{ background: "var(--accent)", borderColor: "var(--accent)", color: "#1a1207", fontWeight: 600 }}
        >
          + Connect repository
        </button>
      </div>

      {/* info banner */}
      <div
        className="dy-card"
        style={{ marginBottom: 16, display: "flex", gap: 9, alignItems: "baseline", fontSize: 13, color: "var(--muted)" }}
      >
        <span style={{ color: "var(--accent-hover)" }}>⎇</span>
        <span>
          dagron polls each repo and reconciles its{" "}
          <code className="mono" style={{ color: "var(--accent-hover)" }}>.dagron/*.yaml</code> definitions into
          live workflows. Out-of-sync means the repo changed but the cluster hasn&apos;t applied it yet.
        </span>
      </div>

      {showForm && (
        <div className="dy-card" style={{ marginBottom: 16, display: "flex", gap: 10, flexWrap: "wrap", alignItems: "center" }}>
          <input
            value={url}
            onChange={(e) => setUrl(e.target.value)}
            placeholder="https://github.com/owner/repo"
            style={{ flex: 1, minWidth: 280, background: "var(--bg)", color: "var(--fg)", border: "1px solid var(--border)", borderRadius: 8, padding: "8px 10px" }}
          />
          <input
            value={branch}
            onChange={(e) => setBranch(e.target.value)}
            placeholder="branch"
            style={{ width: 120, background: "var(--bg)", color: "var(--fg)", border: "1px solid var(--border)", borderRadius: 8, padding: "8px 10px" }}
          />
          <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 13, color: "var(--muted)" }}>
            <input type="checkbox" checked={autoSync} onChange={(e) => setAutoSync(e.target.checked)} /> Auto-sync
          </label>
          <button onClick={onConnect} disabled={busy === "connect"} className="dy-btn dy-btn-primary">
            {busy === "connect" ? "Connecting…" : "Connect"}
          </button>
        </div>
      )}

      {error && <p style={{ color: "var(--red)" }}>{error}</p>}
      {loading && <p className="dy-empty">Loading…</p>}
      {!loading && repos.length === 0 && !error && (
        <p className="dy-empty">No repositories connected. Click “+ Connect repository” to add one.</p>
      )}

      <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
        {repos.map((r) => {
          const color = STATE_COLOR[r.state];
          return (
            <div key={r.id} className="dy-card" style={{ padding: 0, overflow: "hidden" }}>
              {/* top row */}
              <div style={{ display: "flex", alignItems: "center", gap: 11, padding: "16px 18px" }}>
                <span className="dy-dot" style={{ width: 10, height: 10, background: color }} />
                <strong style={{ fontSize: 15 }}>{r.name}</strong>
                <span
                  style={{
                    fontSize: 11,
                    fontWeight: 600,
                    padding: "3px 9px",
                    borderRadius: 999,
                    color,
                    background: STATE_TINT[r.state],
                  }}
                >
                  {r.state}
                </span>
                <div style={{ flex: 1 }} />
                <span style={{ fontSize: 12, color: "var(--muted)", display: "inline-flex", alignItems: "center", gap: 6 }}>
                  <span className="dy-dot" style={{ width: 7, height: 7, background: r.auto_sync ? "var(--green)" : "var(--dim)" }} />
                  Auto-sync {r.auto_sync ? "ON" : "OFF"}
                </span>
                <button onClick={() => onSync(r.id)} disabled={busy === r.id} className="dy-btn">
                  {busy === r.id ? "…" : "Sync"}
                </button>
                <button onClick={() => onDisconnect(r.id)} disabled={busy === r.id} className="dy-btn dy-btn-danger" title="Disconnect">
                  ✕
                </button>
              </div>
              {/* sub row */}
              <div style={{ display: "flex", alignItems: "center", gap: 14, padding: "0 18px 14px 39px", fontSize: 12.5, color: "var(--muted)" }}>
                <span className="mono">⎇ {r.branch}{r.rev ? ` @ ${r.rev}` : ""}</span>
                <span>{r.workflow_count} workflows</span>
                <span>{syncedLabel(r)}</span>
              </div>
              {/* commit row */}
              <div style={{ display: "flex", alignItems: "center", gap: 10, padding: "11px 18px", borderTop: "1px solid var(--border)", fontSize: 13 }}>
                {r.rev && <span className="mono" style={{ color: "var(--blue)" }}>{r.rev}</span>}
                <span style={{ color: "var(--muted)" }}>{r.last_message ?? "—"}</span>
                {r.drift > 0 && (
                  <span style={{ marginLeft: "auto", fontSize: 11, fontWeight: 600, padding: "3px 10px", borderRadius: 999, color: "var(--amber)", background: "rgba(210,153,34,0.13)" }}>
                    {r.drift} drifted
                  </span>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
