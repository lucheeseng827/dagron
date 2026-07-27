"use client";

// Personal access tokens for the signed-in user.
//
// Not admin-gated, on purpose: a token is a credential for whoever is signed
// in, and an operator wiring up CI needs one exactly as much as an admin does.
// Hiding it behind the admin gate would leave them storing a password, which is
// the thing tokens exist to stop.
//
// The screen is built around one awkward fact: the plaintext exists exactly
// once, in the response that creates it. Nothing can retrieve it afterwards
// because only its hash is stored. So the reveal is a deliberate, sticky panel
// that has to be dismissed — not a toast that can scroll away while the user is
// still reaching for their clipboard.

import { useCallback, useEffect, useState } from "react";
import { createToken, listTokens, revokeToken } from "@/lib/dagron-api";
import { errMsg } from "@/lib/err";
import { absTime, timeAgo } from "@/lib/time";
import type { ApiToken, CreatedToken } from "@/types/dagron";

/// Offered expiries. "Never" is last and not the default: a credential that
/// outlives the job it was made for is the one nobody remembers to revoke.
const EXPIRY_CHOICES: { label: string; days: number | null }[] = [
  { label: "30 days", days: 30 },
  { label: "90 days", days: 90 },
  { label: "1 year", days: 365 },
  { label: "Never", days: null },
];

export default function TokensPage() {
  const [tokens, setTokens] = useState<ApiToken[]>([]);
  const [name, setName] = useState("");
  const [days, setDays] = useState<number | null>(90);
  const [created, setCreated] = useState<CreatedToken | null>(null);
  const [copied, setCopied] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [loaded, setLoaded] = useState(false);

  const load = useCallback(() => {
    listTokens()
      .then(setTokens)
      .catch((e) => setError(errMsg(e)))
      .finally(() => setLoaded(true));
  }, []);

  useEffect(() => load(), [load]);

  async function onCreate(e: React.FormEvent) {
    e.preventDefault();
    if (!name.trim() || busy) return;
    setBusy(true);
    setError(null);
    try {
      const t = await createToken(name.trim(), days);
      setCreated(t);
      setCopied(false);
      setName("");
      load();
    } catch (err) {
      setError(errMsg(err));
    } finally {
      setBusy(false);
    }
  }

  async function onRevoke(t: ApiToken) {
    // Revocation is immediate and cannot be undone — the token is dead the
    // moment this returns, so ask before doing it rather than after.
    if (!window.confirm(`Revoke "${t.name}" (${t.prefix})?\n\nAnything using it stops working immediately.`)) {
      return;
    }
    setError(null);
    try {
      await revokeToken(t.id);
      load();
    } catch (err) {
      setError(errMsg(err));
    }
  }

  async function copyToken() {
    if (!created) return;
    try {
      await navigator.clipboard.writeText(created.token);
      setCopied(true);
    } catch {
      // Clipboard access can be denied (insecure origin, permissions). The
      // token is on screen and selectable, so this is a downgrade, not a
      // failure — say so instead of silently doing nothing.
      setError("Couldn't reach the clipboard — select the token above and copy it manually.");
    }
  }

  const live = tokens.filter((t) => !t.revoked_at && !isExpired(t));
  const dead = tokens.filter((t) => t.revoked_at || isExpired(t));

  return (
    <div className="dy-page">
      <div className="dy-pagehead">
        <div>
          <h1 className="dy-h1" style={{ marginBottom: 0 }}>
            API tokens
          </h1>
          <p className="dy-subtitle">
            Long-lived credentials for CI, scripts and the SDKs, so automation never has to
            store your password. Each token carries your own permissions.
          </p>
        </div>
      </div>

      {error && (
        <p style={{ color: "var(--red)", marginBottom: 14 }} role="alert">
          {error}
        </p>
      )}

      {/* The one time the secret is visible. Sticky and dismissible rather than
          a toast: it cannot be shown again, so it must not be possible to lose
          it by scrolling or by clicking somewhere else. */}
      {created && (
        <div
          className="dy-card"
          style={{ borderColor: "var(--accent)", marginBottom: 18 }}
          role="status"
        >
          <div className="dy-cardhead">
            <strong>Copy “{created.name}” now</strong>
            <button className="dy-btn" onClick={() => setCreated(null)}>
              Done
            </button>
          </div>
          <p style={{ color: "var(--muted)", fontSize: 13.5, margin: "0 0 10px" }}>
            This is the only time it is shown. Only a hash is stored, so it cannot be
            retrieved later — if you lose it, revoke this token and make another.
          </p>
          <div style={{ display: "flex", gap: 8, alignItems: "stretch" }}>
            <code
              className="mono"
              style={{
                flex: 1,
                background: "var(--bg)",
                border: "1px solid var(--border)",
                borderRadius: 6,
                padding: "9px 11px",
                fontSize: 13,
                wordBreak: "break-all",
                userSelect: "all",
              }}
            >
              {created.token}
            </code>
            <button className="dy-btn dy-btn-primary" onClick={copyToken} style={{ flexShrink: 0 }}>
              {copied ? "Copied" : "Copy"}
            </button>
          </div>
        </div>
      )}

      <form onSubmit={onCreate} className="dy-card" style={{ marginBottom: 18 }}>
        <div className="dy-cardhead">
          <strong>New token</strong>
        </div>
        <div style={{ display: "flex", gap: 10, flexWrap: "wrap", alignItems: "flex-end" }}>
          <label style={{ flex: "1 1 240px", fontSize: 12, color: "var(--muted)" }}>
            Name
            <input
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="nightly-ci"
              // What it is for, so a future you can tell which to revoke.
              title="What this token is for — shown in the list below"
              style={inputStyle}
            />
          </label>
          <label style={{ flex: "0 1 160px", fontSize: 12, color: "var(--muted)" }}>
            Expires
            <select
              value={days === null ? "never" : String(days)}
              onChange={(e) => setDays(e.target.value === "never" ? null : Number(e.target.value))}
              style={inputStyle}
            >
              {EXPIRY_CHOICES.map((c) => (
                <option key={c.label} value={c.days === null ? "never" : String(c.days)}>
                  {c.label}
                </option>
              ))}
            </select>
          </label>
          <button
            type="submit"
            className="dy-btn dy-btn-primary"
            disabled={busy || !name.trim()}
            style={{ marginBottom: 1 }}
          >
            {busy ? "Creating…" : "Create token"}
          </button>
        </div>
      </form>

      <TokenTable
        title="Active"
        rows={live}
        loaded={loaded}
        empty="No active tokens. Create one above to authenticate CI or a script."
        onRevoke={onRevoke}
      />
      {dead.length > 0 && (
        <TokenTable
          title="Revoked and expired"
          rows={dead}
          loaded
          // Kept rather than deleted: after an incident the question is what
          // existed and when it stopped, and a deleted row cannot answer it.
          empty=""
          onRevoke={null}
        />
      )}
    </div>
  );
}

function TokenTable({
  title,
  rows,
  loaded,
  empty,
  onRevoke,
}: {
  title: string;
  rows: ApiToken[];
  loaded: boolean;
  empty: string;
  onRevoke: ((t: ApiToken) => void) | null;
}) {
  return (
    <div className="dy-card" style={{ marginBottom: 18 }}>
      <div className="dy-cardhead">
        <strong>{title}</strong>
        <span style={{ color: "var(--dim)", fontSize: 12 }}>{rows.length}</span>
      </div>
      {!loaded ? (
        <p className="dy-empty">Loading…</p>
      ) : rows.length === 0 ? (
        <p className="dy-empty">{empty}</p>
      ) : (
        <div style={{ overflowX: "auto" }}>
          <table className="dy-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Token</th>
                <th>Last used</th>
                <th>Expires</th>
                {onRevoke && <th />}
              </tr>
            </thead>
            <tbody>
              {rows.map((t) => (
                <tr key={t.id}>
                  <td>{t.name}</td>
                  <td className="mono" style={{ fontSize: 12.5, color: "var(--muted)" }}>
                    {/* The prefix only — enough to match against the token you
                        copied, useless as a credential. */}
                    {t.prefix}…
                  </td>
                  <td style={{ color: t.last_used_at ? undefined : "var(--dim)" }}>
                    {t.last_used_at ? (
                      <span title={absTime(t.last_used_at)}>{timeAgo(t.last_used_at)}</span>
                    ) : (
                      "never used"
                    )}
                  </td>
                  <td style={{ color: "var(--muted)" }}>{expiryLabel(t)}</td>
                  {onRevoke && (
                    <td style={{ textAlign: "right" }}>
                      <button className="dy-btn dy-btn-danger" onClick={() => onRevoke(t)}>
                        Revoke
                      </button>
                    </td>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function isExpired(t: ApiToken): boolean {
  if (!t.expires_at) return false;
  const at = new Date(t.expires_at).getTime();
  return !Number.isNaN(at) && at <= Date.now();
}

function expiryLabel(t: ApiToken): string {
  if (t.revoked_at) return `revoked ${timeAgo(t.revoked_at)}`;
  if (!t.expires_at) return "never";
  return isExpired(t) ? "expired" : absTime(t.expires_at).slice(0, 10);
}

const inputStyle: React.CSSProperties = {
  display: "block",
  width: "100%",
  marginTop: 4,
  background: "var(--bg)",
  color: "var(--fg)",
  border: "1px solid var(--border)",
  borderRadius: 6,
  padding: "7px 9px",
  fontSize: 13,
};
