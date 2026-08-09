"use client";

// Thin route over the audit-log screen. `@ee/AuditLogView` resolves to the
// enterprise implementation (src/ee) when present, else to the stub in
// src/ee-stub, so this file compiles either way.

import AuditLogView from "@ee/AuditLogView";

export default function AuditPage() {
  return <AuditLogView />;
}
