"use client";

import { useCallback, useEffect, useState } from "react";
import {
  clearGitRepoAuth,
  connectGitRepo,
  disconnectGitRepo,
  listGitRepos,
  setGitRepoAuth,
  syncGitRepo,
} from "@/lib/dagron-api";
import GitCredentialFields, {
  emptyCredential,
  type GitCredentialValue,
} from "@/components/GitCredentialFields";
import { errMsg } from "@/lib/err";
import { timeAgo } from "@/lib/time";
import type { GitAuthInput, GitRepo, GitRepoState } from "@/types/dagron";

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

/// The credential form's state as the API's write-only payload, or `undefined`
/// when no credential is being set. Empty fields are dropped rather than sent as
/// `""`, so submitting the form with the token box untouched is a no-op instead
/// of storing a blank secret.
function toAuthInput(v: GitCredentialValue): GitAuthInput | undefined {
  if (v.kind === "none") return undefined;
  const trim = (s: string) => (s.trim() ? s.trim() : undefined);
  return {
    kind: v.kind,
    username: trim(v.username),
    token: v.kind === "token" ? trim(v.token) : undefined,
    // Not trimmed: a PEM key is whitespace-significant beyond its edges. Only
    // the surrounding blank lines a paste adds are removed.
    ssh_private_key: v.kind === "ssh" ? v.sshPrivateKey.trim() || undefined : undefined,
    known_hosts: v.kind === "ssh" ? trim(v.knownHosts) : undefined,
  };
}

/// One-line description of the credential installed on a repo.
function authLabel(r: GitRepo): string {
  if (r.auth_kind === "token") return `HTTPS token ${r.auth_hint ?? ""}`.trim();
  if (r.auth_kind === "ssh") {
    return `SSH key ${r.auth_hint ?? ""}${r.auth_known_hosts ? "" : " · host key unverified"}`.trim();
  }
  return "No credential";
}

export default function GitOpsPage() {
  const [repos, setRepos] = useState<GitRepo[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState<string | null>(null);
  const [showForm, setShowForm] = useState(false);
  // Null until the first load answers; only `false` means "definitely absent".
  const [workerOnline, setWorkerOnline] = useState<boolean | null>(null);
  // Same "null until the first load answers" rule: never warn about missing
  // encryption on the strength of not having asked yet.
  const [credentialsConfigured, setCredentialsConfigured] = useState<boolean | null>(null);
  const [url, setUrl] = useState("");
  const [branch, setBranch] = useState("main");
  const [autoSync, setAutoSync] = useState(true);
  const [cred, setCred] = useState<GitCredentialValue>(emptyCredential());
  // Repo whose credential editor is open, and its draft. Held per-repo rather
  // than globally so opening a second one cannot submit the first one's key.
  const [authFor, setAuthFor] = useState<string | null>(null);
  const [authDraft, setAuthDraft] = useState<GitCredentialValue>(emptyCredential());

  const load = useCallback(() => {
    listGitRepos()
      .then((r) => {
        setRepos(r.repos);
        setWorkerOnline(r.worker_online);
        setCredentialsConfigured(r.credentials_configured);
      })
      .catch((e) => setError(errMsg(e)))
      .finally(() => setLoading(false));
  }, []);
  useEffect(() => load(), [load]);

  const onConnect = async () => {
    if (!url.trim()) return;
    setBusy("connect");
    setError(null);
    try {
      await connectGitRepo({
        url: url.trim(),
        branch: branch.trim() || "main",
        auto_sync: autoSync,
        auth: toAuthInput(cred),
      });
      setUrl("");
      // Clear the credential from component state on success — there is no
      // reason for a private key to outlive the request that stored it.
      setCred(emptyCredential());
      setShowForm(false);
      load();
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setBusy(null);
    }
  };

  const onSaveAuth = async (r: GitRepo) => {
    setBusy(r.id);
    setError(null);
    try {
      const auth = toAuthInput(authDraft);
      if (auth) {
        await setGitRepoAuth(r.id, auth);
      } else {
        await clearGitRepoAuth(r.id);
      }
      setAuthFor(null);
      setAuthDraft(emptyCredential());
      load();
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setBusy(null);
    }
  };

  /// Open the editor seeded from what is already stored: the kind and the
  /// known_hosts (both public), never the secret — it cannot be read back, and
  /// pre-filling a masked value would only invite saving the mask.
  const openAuth = (r: GitRepo) => {
    setAuthFor(r.id);
    setAuthDraft({
      ...emptyCredential(),
      kind: r.auth_kind,
      username: r.auth_username ?? "",
      knownHosts: r.auth_known_hosts ?? "",
    });
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
        <div className="dy-card" style={{ marginBottom: 16, display: "flex", flexDirection: "column", gap: 12 }}>
          <div style={{ display: "flex", gap: 10, flexWrap: "wrap", alignItems: "center" }}>
            <input
              value={url}
              onChange={(e) => setUrl(e.target.value)}
              placeholder="https://github.com/owner/repo or git@github.com:owner/repo.git"
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
          </div>
          {credentialsConfigured === false ? (
            <p style={{ fontSize: 12.5, color: "var(--muted)", margin: 0 }}>
              Private repositories need credential storage: set{" "}
              <code className="mono">DAGRON_ENV_SECRET_KEY</code> on dagron-api and the{" "}
              <code className="mono">dagron-gitops</code> worker, then reload this page.
            </p>
          ) : (
            <GitCredentialFields url={url} value={cred} onChange={setCred} disabled={busy === "connect"} />
          )}
          <div>
            <button onClick={onConnect} disabled={busy === "connect"} className="dy-btn dy-btn-primary">
              {busy === "connect" ? "Connecting…" : "Connect"}
            </button>
          </div>
        </div>
      )}

      {error && <p style={{ color: "var(--red)" }}>{error}</p>}
      {loading && <p className="dy-empty">Loading…</p>}
      {workerOnline === false && (
        <div
          style={{
            border: "1px solid rgba(210,153,34,0.45)",
            background: "rgba(210,153,34,0.10)",
            borderRadius: 8,
            padding: "0.75rem 0.9rem",
            marginBottom: 12,
            fontSize: 13,
          }}
        >
          <strong style={{ color: "var(--yellow, #d29922)" }}>No GitOps worker running.</strong>{" "}
          Repositories below will not sync until the <code>dagron-gitops</code> container is
          deployed — syncing runs there, not in the API. Compose:{" "}
          <code>podman compose --profile gitops up -d</code>.
        </div>
      )}
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
                <button
                  onClick={() => (authFor === r.id ? setAuthFor(null) : openAuth(r))}
                  disabled={busy === r.id}
                  className="dy-btn"
                  title="Set or rotate this repository's Git credential"
                >
                  {r.auth_kind === "none" ? "🔑 Add credential" : "🔑 Credential"}
                </button>
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
                <span
                  title={
                    r.auth_updated_at ? `credential updated ${timeAgo(r.auth_updated_at)}` : undefined
                  }
                  style={{ color: r.auth_kind === "none" ? "var(--muted)" : "var(--fg)" }}
                >
                  {r.auth_kind === "none" ? "🔓" : "🔒"} {authLabel(r)}
                </span>
              </div>

              {authFor === r.id && (
                <div
                  style={{
                    padding: "14px 18px",
                    borderTop: "1px solid var(--border)",
                    display: "flex",
                    flexDirection: "column",
                    gap: 12,
                  }}
                >
                  {credentialsConfigured === false ? (
                    <p style={{ fontSize: 12.5, color: "var(--muted)", margin: 0 }}>
                      Credential storage is not configured: set{" "}
                      <code className="mono">DAGRON_ENV_SECRET_KEY</code> on dagron-api and the{" "}
                      <code className="mono">dagron-gitops</code> worker.
                    </p>
                  ) : (
                    <>
                      <GitCredentialFields
                        url={r.url}
                        value={authDraft}
                        onChange={setAuthDraft}
                        disabled={busy === r.id}
                      />
                      <p style={{ fontSize: 11.5, color: "var(--muted)", margin: 0 }}>
                        Stored encrypted and never shown again. Saving replaces whatever is
                        installed; choosing <em>Public / none</em> removes it.
                      </p>
                    </>
                  )}
                  <div style={{ display: "flex", gap: 8 }}>
                    <button
                      onClick={() => onSaveAuth(r)}
                      disabled={busy === r.id || credentialsConfigured === false}
                      className="dy-btn dy-btn-primary"
                    >
                      {busy === r.id ? "Saving…" : "Save credential"}
                    </button>
                    <button onClick={() => setAuthFor(null)} disabled={busy === r.id} className="dy-btn">
                      Cancel
                    </button>
                  </div>
                </div>
              )}
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
