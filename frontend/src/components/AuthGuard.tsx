"use client";

import { useEffect, useState } from "react";
import CommandPalette from "@/components/CommandPalette";
import Sidebar from "@/components/Sidebar";
import { checkSession, login } from "@/lib/dagron-api";

// Gate the app on a valid session. The token lives in an HttpOnly cookie that JS
// can't read, so we probe `/api/me` (the cookie rides along automatically) to
// decide between the app, the sign-in form, and a backend-unavailable notice.
// Login sets state explicitly and sign-out forces a reload, so the probe only
// needs to run on mount (and on an explicit retry) — not on every navigation.

export default function AuthGuard({ children }: { children: React.ReactNode }) {
  const [state, setState] = useState<"checking" | "ok" | "needs-token" | "error">("checking");
  const [probe, setProbe] = useState(0);

  useEffect(() => {
    let cancelled = false;
    setState("checking");
    void checkSession().then((s) => {
      if (cancelled) return;
      setState(s === "authed" ? "ok" : s === "unauthed" ? "needs-token" : "error");
    });
    return () => {
      cancelled = true;
    };
  }, [probe]);

  if (state === "checking") return null;
  if (state === "error") return <UnavailableGate onRetry={() => setProbe((n) => n + 1)} />;
  if (state === "needs-token") return <SignInGate onSet={() => setState("ok")} />;

  return (
    <div className="dy-shell">
      <Sidebar />
      <main className="dy-main">{children}</main>
      {/* Global ⌘K search — only mounted once the session is valid. */}
      <CommandPalette />
    </div>
  );
}

/// Shown when `/api/me` can't be reached (network/proxy/5xx). Distinct from a
/// real sign-out so a transient backend blip doesn't masquerade as a logout.
function UnavailableGate({ onRetry }: { onRetry: () => void }) {
  return (
    <div style={{ maxWidth: 460, margin: "5rem auto", padding: "1.5rem" }}>
      <h1 className="dy-h1">Service unavailable</h1>
      <div className="dy-card">
        <p style={{ color: "var(--muted)", lineHeight: 1.6 }}>
          Couldn’t reach dagron-api — this is usually temporary.
        </p>
        <button
          type="button"
          className="dy-btn dy-btn-primary"
          style={{ marginTop: 12 }}
          onClick={onRetry}
        >
          Retry
        </button>
      </div>
    </div>
  );
}

/// Sign-in screen: email + password, exchanged for a session token by dagron-api.
function SignInGate({ onSet }: { onSet: () => void }) {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const inputStyle = {
    width: "100%",
    marginTop: 12,
    background: "var(--bg)",
    color: "var(--fg)",
    border: "1px solid var(--border)",
    borderRadius: 8,
    padding: "8px 10px",
  } as const;

  const labelStyle = {
    display: "block",
    marginTop: 12,
    fontSize: 13,
    color: "var(--muted)",
  } as const;

  async function submit() {
    if (!email || !password || busy) return;
    setBusy(true);
    setError(null);
    try {
      await login(email, password);
      onSet();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Sign-in failed.");
      setBusy(false);
    }
  }

  return (
    <div style={{ maxWidth: 460, margin: "5rem auto", padding: "1.5rem" }}>
      <h1 className="dy-h1">Sign in</h1>
      <form
        className="dy-card"
        onSubmit={(e) => {
          e.preventDefault();
          void submit();
        }}
      >
        <p style={{ color: "var(--muted)", lineHeight: 1.6 }}>
          Sign in to dagron with your email and password.
        </p>
        <label htmlFor="signin-email" style={labelStyle}>
          Email
        </label>
        <input
          id="signin-email"
          type="email"
          autoComplete="username"
          placeholder="you@example.com"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          style={inputStyle}
        />
        <label htmlFor="signin-password" style={labelStyle}>
          Password
        </label>
        <input
          id="signin-password"
          type="password"
          autoComplete="current-password"
          placeholder="Password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          style={inputStyle}
        />
        {error && (
          <p style={{ color: "var(--danger, #f85149)", marginTop: 12, fontSize: 13 }}>{error}</p>
        )}
        <button
          type="submit"
          className="dy-btn dy-btn-primary"
          style={{ marginTop: 12 }}
          disabled={busy}
        >
          {busy ? "Signing in…" : "Sign in"}
        </button>
      </form>
    </div>
  );
}
