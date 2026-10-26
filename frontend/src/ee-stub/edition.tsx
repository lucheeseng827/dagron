// Fallback for the edition mark. Ships in the public mirror; the private
// src/ee/edition.tsx, resolved first by the tsconfig `@ee/*` path fallback,
// replaces it in builds made from the closed tree. Keep the exports identical.
//
// No "use client": the root layout, a server component, reads PRODUCT_NAME for
// the tab's title.

/// What this console calls itself, in the tab's title and on the sign-in form.
export const PRODUCT_NAME = "dagron";

/// A mark that says which edition this build is: beside the name, or on the
/// logo's tile when the sidebar is collapsed to a rail. None, in this build.
export function EditionMark(_props: { where?: "name" | "tile" }) {
  return null;
}
