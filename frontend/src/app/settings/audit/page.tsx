"use client";

// Thin route over the audit-log screen. `@ee/AuditLogView` resolves to the
// enterprise implementation (src/ee) when present, else to the OSS upsell
// stub (src/ee-stub) — the open-core seam from docs/OPEN_SOURCE.md §2, so
// this file ships in the public mirror unchanged.

import AuditLogView from "@ee/AuditLogView";

export default function AuditPage() {
  return <AuditLogView />;
}
