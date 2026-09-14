"use client";

// OSS fallback for the enterprise "build from recipe" control on a task's
// Docker-image field. Ships in the public mirror; the private repo's
// src/ee/RecipeImageField.tsx (resolved first by the tsconfig `@ee/*` path
// fallback) replaces it in enterprise builds. Keep the default-export contract
// identical to the real control.
//
// A signpost, not a wall (docs/OPEN_SOURCE.md): it names what the enterprise
// build does here and what this build does instead, in the product's words. It
// is deliberately one line rather than a card — this sits under a form field
// inside a side panel, and a banner there would be shouting.

import type { ImageFieldContext } from "@/components/dag/EditableDag";

export default function RecipeImageField(_ctx: ImageFieldContext) {
  return (
    <p style={{ margin: "4px 0 0", fontSize: 12, lineHeight: 1.5, color: "var(--muted)" }}>
      The enterprise build can also describe an image here — packages, files, a base — and build
      it as part of this workflow. In this build, name an image your registry already has.
    </p>
  );
}
