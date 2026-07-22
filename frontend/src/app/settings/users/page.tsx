"use client";

// Admin user management: list accounts + create new ones with a role.
// Roles: admin (everything incl. user management), operator (default —
// full control-plane access), viewer (read-only, enforced server-side).

import { useCallback, useEffect, useState } from "react";
import { useToast } from "@/components/Toasts";
import { createUser, getHealth, getMe, listUsers, type Me } from "@/lib/dagron-api";
import { errMsg } from "@/lib/err";
import { absTime, timeAgo } from "@/lib/time";
import type { UserView } from "@/types/dagron";

type Role = "operator" | "admin" | "viewer";

export default function UsersPage() {
  const toast = useToast();
  const [me, setMe] = useState<Me | null>(null);
  const [users, setUsers] = useState<UserView[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  // The read-only `viewer` role is enforced by the enterprise build only —
  // hide it on OSS so an unenforced role can't be handed out.
  const [isEnterprise, setIsEnterprise] = useState(false);

  const [email, setEmail] = useState("");
  const [name, setName] = useState("");
  const [password, setPassword] = useState("");
  const [role, setRole] = useState<Role>("operator");

  const load = useCallback(() => {
    listUsers()
      .then((u) => {
        setUsers(u);
        setError(null);
      })
      .catch((e) => setError(errMsg(e)));
  }, []);

  useEffect(() => {
    getMe().then(setMe).catch(() => {});
    getHealth()
      .then((h) => setIsEnterprise(h.edition === "enterprise"))
      .catch(() => {});
    load();
  }, [load]);

  const isAdmin = me?.groups?.includes("admin") ?? false;

  const onCreate = async () => {
    setBusy(true);
    try {
      // "operator" is the default posture — no explicit group needed.
      const groups = role === "admin" ? ["admin"] : role === "viewer" ? ["viewer"] : [];
      await createUser(email.trim(), name.trim(), password, groups);
      toast(`User ${email.trim()} created`);
      setEmail("");
      setName("");
      setPassword("");
      load();
    } catch (e) {
      toast(errMsg(e), "error");
    } finally {
      setBusy(false);
    }
  };

  const roleOf = (u: UserView): Role =>
    u.groups.includes("admin") ? "admin" : u.groups.includes("viewer") ? "viewer" : "operator";

  return (
    <div className="dy-page">
      <div className="dy-pagehead">
        <div>
          <h1 className="dy-h1" style={{ marginBottom: 0 }}>
            Users
          </h1>
          <p className="dy-subtitle">
            Accounts for this dagron instance. Viewers are read-only; admins can manage users.
          </p>
        </div>
      </div>

      {me && !isAdmin && (
        <div className="dy-card" style={{ borderColor: "var(--amber)" }}>
          <p style={{ margin: 0, color: "var(--amber)" }}>The admin group is required to manage users.</p>
        </div>
      )}
      {error && isAdmin && <p style={{ color: "var(--red)" }}>{error}</p>}

      {isAdmin && (
        <>
          <div className="dy-card" style={{ marginBottom: 18 }}>
            <strong>New user</strong>
            <div style={{ display: "flex", gap: 10, alignItems: "flex-end", flexWrap: "wrap", marginTop: 12 }}>
              <Field label="Email">
                <input value={email} onChange={(e) => setEmail(e.target.value)} type="email" placeholder="ada@example.com" className="dy-btn" style={{ minWidth: 220 }} />
              </Field>
              <Field label="Name">
                <input value={name} onChange={(e) => setName(e.target.value)} placeholder="Ada Lovelace" className="dy-btn" style={{ minWidth: 180 }} />
              </Field>
              <Field label="Password (min 8 chars)">
                <input value={password} onChange={(e) => setPassword(e.target.value)} type="password" className="dy-btn" style={{ minWidth: 160 }} />
              </Field>
              <Field label="Role">
                <select value={role} onChange={(e) => setRole(e.target.value as Role)} className="dy-btn" style={{ cursor: "pointer" }}>
                  <option value="operator">Operator — full control</option>
                  {isEnterprise && <option value="viewer">Viewer — read-only (Enterprise)</option>}
                  <option value="admin">Admin — control + user management</option>
                </select>
              </Field>
              <button
                onClick={onCreate}
                disabled={busy || !email.trim() || !name.trim() || password.length < 8}
                className="dy-btn dy-btn-primary"
              >
                Create user
              </button>
            </div>
          </div>

          <div className="dy-card" style={{ padding: 0, overflow: "hidden" }}>
            <div style={{ display: "grid", gridTemplateColumns: "1.4fr 1.2fr 0.8fr 1fr", gap: 12, padding: "11px 18px", borderBottom: "1px solid var(--border)", fontSize: 11, fontWeight: 600, color: "var(--dim)", textTransform: "uppercase", letterSpacing: "0.05em" }}>
              <div>Email</div>
              <div>Name</div>
              <div>Role</div>
              <div>Created</div>
            </div>
            {users.map((u) => (
              <div key={u.id} style={{ display: "grid", gridTemplateColumns: "1.4fr 1.2fr 0.8fr 1fr", gap: 12, padding: "13px 18px", borderBottom: "1px solid var(--border)", fontSize: 13, alignItems: "center" }}>
                <span className="mono">{u.email}</span>
                <span>{u.name}</span>
                <span className="dy-pill" style={{ justifySelf: "start", color: roleOf(u) === "admin" ? "var(--accent)" : roleOf(u) === "viewer" ? "var(--muted)" : "var(--blue)" }}>
                  {roleOf(u)}
                </span>
                <span style={{ color: "var(--muted)" }} title={absTime(u.created_at)}>
                  {timeAgo(u.created_at)}
                </span>
              </div>
            ))}
            {users.length === 0 && !error && <p className="dy-empty" style={{ padding: 16 }}>No users.</p>}
          </div>
        </>
      )}
    </div>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label style={{ fontSize: 12, color: "var(--muted)" }}>
      {label}
      <br />
      <span style={{ display: "inline-block", marginTop: 4 }}>{children}</span>
    </label>
  );
}
