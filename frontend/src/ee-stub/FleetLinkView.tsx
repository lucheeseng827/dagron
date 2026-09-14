"use client";

// OSS fallback for the enterprise fleet-link screen. Ships in the public
// mirror; the private repo's src/ee/FleetLinkView.tsx (resolved first by the
// tsconfig `@ee/*` path fallback) replaces it in enterprise builds. Keep the
// default-export contract identical to the real view.
//
// A signpost, not a wall (docs/OPEN_SOURCE.md): it names what the enterprise
// build does here and what this build does instead, in the product's words.

export default function FleetLinkView() {
  return (
    <div className="dy-page" style={{ maxWidth: 720 }}>
      <div className="dy-pagehead">
        <div>
          <h1 className="dy-h1" style={{ marginBottom: 0 }}>
            Fleet link
          </h1>
          <p className="dy-subtitle">Enrolling this instance as a unit of a control plane&apos;s org.</p>
        </div>
      </div>
      <div className="dy-card">
        <p style={{ margin: "0 0 10px", lineHeight: 1.55 }}>
          Not in this build. The enterprise build enrols an instance with a join token, opens one
          outbound channel to the control plane, and shows the offline licence it runs under.
        </p>
        <p style={{ margin: 0, color: "var(--muted)", fontSize: 13.5, lineHeight: 1.55 }}>
          This build runs one instance on its own: drive it through this API,{" "}
          <code className="mono">SOURCE=dir</code>, or GitOps sync. See{" "}
          <code className="mono">docs/OPERATIONS.md</code> and the README section{" "}
          <em>what this build does not do</em>.
        </p>
      </div>
    </div>
  );
}
