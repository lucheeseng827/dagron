"use client";

// Fallback for the hosted-service additions to this UI (workspace switching,
// single sign-on). Ships in the public mirror; the private src/ee/cloud.tsx,
// resolved first by the tsconfig `@ee/*` path fallback, replaces it in hosted
// builds. Keep the exports identical.

/// Whether this UI is served by the hosted service. Never, in this build.
export function useIsCloud(): boolean {
  return false;
}

export function CloudWorkspaceSwitcher(_props: { rail?: boolean }) {
  return null;
}

export function CloudSignInExtras() {
  return null;
}
