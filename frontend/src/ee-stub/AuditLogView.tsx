"use client";

// OSS fallback for the enterprise audit-log screen. Ships in the public
// mirror; the private repo's src/ee/AuditLogView.tsx (resolved first by the
// tsconfig `@ee/*` path fallback) replaces it in enterprise builds. Keep the
// default-export contract identical to the real view.

export default function AuditLogView() {
  return (
    <div className="dy-page" style={{ maxWidth: 720 }}>
      <div className="dy-pagehead">
        <div>
          <h1 className="dy-h1" style={{ marginBottom: 0 }}>
            Audit log
          </h1>
          <p className="dy-subtitle">Compliance-grade change history for the control plane.</p>
        </div>
      </div>
      <div className="dy-card">
        <p style={{ margin: 0, lineHeight: 1.7, color: "var(--muted)" }}>
          The audit trail — every successful mutation recorded with who / what / when, plus the
          read-only <code className="mono">viewer</code> role — is part of{" "}
          <strong>dagron Enterprise</strong>. This build (OSS) does not record or serve audit
          entries.
        </p>
      </div>
    </div>
  );
}
