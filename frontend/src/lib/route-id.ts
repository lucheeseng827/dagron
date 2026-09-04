// Read the `?id=` a detail page was opened with.
//
// These pages used to be dynamic segments (`/runs/[id]`). A statically exported
// site cannot have them: Next resolves dynamic segments at build time via
// generateStaticParams, and a run id is not knowable then. Query parameters are
// resolved in the browser, which is where these pages already did all their work
// — every one of them is a "use client" component that fetches through
// `BASE = "/api"` after mount.
//
// Callers must render this inside a <Suspense> boundary: useSearchParams()
// suspends during prerender, and Next fails the export build without one.
"use client";

import { useSearchParams } from "next/navigation";

export function useRouteId(): string {
  return useSearchParams().get("id") ?? "";
}
