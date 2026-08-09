"use client";

import { useEffect, useId } from "react";

import type { GitAuthKind } from "@/types/dagron";

/// The credential half of the GitOps forms, shared by "connect a repository"
/// and "rotate this repository's credential" so the two cannot drift into
/// asking for different things.
///
/// Which kinds are offered is decided by the URL, not by the operator: an
/// `ssh://` (or `git@host:`) repo can only take a key, an `https://` one only a
/// token. The API enforces the same rule, but a form that lets you fill in a
/// credential and *then* rejects it has already wasted the one paste that
/// mattered — a private key does not survive a round trip through an error page.

export interface GitCredentialValue {
  kind: GitAuthKind;
  username: string;
  token: string;
  sshPrivateKey: string;
  knownHosts: string;
}

export const emptyCredential = (): GitCredentialValue => ({
  kind: "none",
  username: "",
  token: "",
  sshPrivateKey: "",
  knownHosts: "",
});

/// Which credential kinds a URL can carry. An empty/unparseable URL allows both
/// so the selector is not disabled before anything is typed.
export function allowedKinds(url: string): GitAuthKind[] {
  const u = url.trim();
  if (!u) return ["token", "ssh"];
  if (u.startsWith("ssh://")) return ["ssh"];
  if (u.startsWith("https://") || u.startsWith("http://")) return ["token"];
  // scp-style `git@host:owner/repo` — the form a forge's clone button gives you.
  if (/^[\w.-]+@[\w.-]+:/.test(u) || (!u.includes("://") && u.includes(":"))) return ["ssh"];
  return ["token", "ssh"];
}

const input: React.CSSProperties = {
  background: "var(--bg)",
  color: "var(--fg)",
  border: "1px solid var(--border)",
  borderRadius: 8,
  padding: "8px 10px",
  width: "100%",
};

const label: React.CSSProperties = {
  fontSize: 12,
  color: "var(--muted)",
  display: "block",
  marginBottom: 4,
};

export default function GitCredentialFields({
  url,
  value,
  onChange,
  disabled,
}: {
  url: string;
  value: GitCredentialValue;
  onChange: (v: GitCredentialValue) => void;
  disabled?: boolean;
}) {
  const kinds = allowedKinds(url);
  const set = (patch: Partial<GitCredentialValue>) => onChange({ ...value, ...patch });
  // A URL change can strand the form on a kind that URL cannot use — an SSH key
  // drafted against `git@host:…` that then becomes an `https://` URL. Rendering
  // "none" while the parent still holds `kind: "ssh"` would submit the key
  // anyway and take a 400; reset the draft instead, which also drops the secret
  // that can no longer be used.
  const stranded = value.kind !== "none" && !kinds.includes(value.kind);
  useEffect(() => {
    if (stranded) onChange(emptyCredential());
  }, [stranded, onChange]);
  const kind: GitAuthKind = stranded ? "none" : value.kind;

  // Radios group by `name` across the whole document, and the connect form and a
  // repository's editor can be open at once — one shared name let a choice in
  // either clear the other's selection.
  const groupName = useId();

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10, width: "100%" }}>
      <div style={{ display: "flex", gap: 16, alignItems: "center", flexWrap: "wrap" }}>
        <span style={{ fontSize: 12, color: "var(--muted)" }}>Authentication</span>
        {(["none", ...kinds] as GitAuthKind[]).map((k) => (
          <label
            key={k}
            style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 13, cursor: "pointer" }}
          >
            <input
              type="radio"
              name={groupName}
              checked={kind === k}
              disabled={disabled}
              onChange={() => set({ kind: k })}
            />
            {k === "none" ? "Public / none" : k === "token" ? "HTTPS token" : "SSH key"}
          </label>
        ))}
      </div>

      {kind === "token" && (
        <>
          <div>
            <label style={label}>Token</label>
            <input
              type="password"
              autoComplete="new-password"
              value={value.token}
              disabled={disabled}
              onChange={(e) => set({ token: e.target.value })}
              placeholder="ghp_… / glpat-… — a PAT or deploy token with read access"
              style={input}
            />
          </div>
          <div>
            <label style={label}>Username (optional)</label>
            <input
              value={value.username}
              disabled={disabled}
              onChange={(e) => set({ username: e.target.value })}
              placeholder="x-access-token"
              style={input}
            />
          </div>
        </>
      )}

      {kind === "ssh" && (
        <>
          <div>
            <label style={label}>Private key</label>
            <textarea
              value={value.sshPrivateKey}
              disabled={disabled}
              onChange={(e) => set({ sshPrivateKey: e.target.value })}
              placeholder={"-----BEGIN OPENSSH PRIVATE KEY-----\n…"}
              rows={6}
              className="mono"
              style={{ ...input, resize: "vertical", fontSize: 12 }}
            />
            <span style={{ fontSize: 11.5, color: "var(--muted)" }}>
              The private half of a deploy key, with no passphrase — the worker has no terminal to
              be prompted at.
            </span>
          </div>
          <div>
            <label style={label}>Known hosts (recommended)</label>
            <textarea
              value={value.knownHosts}
              disabled={disabled}
              onChange={(e) => set({ knownHosts: e.target.value })}
              placeholder="github.com ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAA…"
              rows={2}
              className="mono"
              style={{ ...input, resize: "vertical", fontSize: 12 }}
            />
            <span style={{ fontSize: 11.5, color: "var(--muted)" }}>
              Take these from the forge&apos;s published host-key fingerprints. If you collect them
              with <code className="mono">ssh-keyscan {hostOf(url) || "your-forge"}</code>, compare
              the fingerprint against the forge before saving — keyscan reports whatever answers on
              the network, it does not authenticate it. Without any entry the host key is not
              verified at all.
            </span>
          </div>
          <div>
            <label style={label}>SSH user (optional)</label>
            <input
              value={value.username}
              disabled={disabled}
              onChange={(e) => set({ username: e.target.value })}
              placeholder="git"
              style={input}
            />
          </div>
        </>
      )}
    </div>
  );
}

/// Host portion of a repo URL, for the `ssh-keyscan` hint. Best-effort: the hint
/// falls back to a placeholder rather than showing something wrong.
function hostOf(url: string): string {
  const u = url.trim();
  if (!u) return "";
  const afterScheme = u.includes("://") ? u.slice(u.indexOf("://") + 3) : u;
  const authority = afterScheme.split(/[/:]/)[0] ?? "";
  const host = authority.includes("@") ? authority.slice(authority.indexOf("@") + 1) : authority;
  return /^[\w.-]+$/.test(host) ? host : "";
}
