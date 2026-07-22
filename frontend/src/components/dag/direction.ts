"use client";

// The DAG's layout direction: classic top-down, left-to-right, or a diagonal
// cascade. Picked from the segmented control on the graph and remembered
// across pages and sessions via localStorage.

import { useCallback, useEffect, useState } from "react";

export type LayoutDirection = "TB" | "LR" | "DG";

export const DIRECTIONS: { id: LayoutDirection; glyph: string; label: string }[] = [
  { id: "TB", glyph: "↓", label: "Vertical layout (top to bottom)" },
  { id: "LR", glyph: "→", label: "Horizontal layout (left to right)" },
  { id: "DG", glyph: "↘", label: "Diagonal cascade layout" },
];

const STORAGE_KEY = "dagron.dag-direction";

function isDirection(v: unknown): v is LayoutDirection {
  return v === "TB" || v === "LR" || v === "DG";
}

/// Layout-direction state persisted to localStorage. The stored value is read
/// after mount (SSR-safe: the server always paints the TB default), and every
/// change is written back so the choice follows the user across views.
export function useDagDirection(): [LayoutDirection, (d: LayoutDirection) => void] {
  const [dir, setDir] = useState<LayoutDirection>("TB");
  useEffect(() => {
    try {
      const stored = window.localStorage.getItem(STORAGE_KEY);
      if (isDirection(stored)) setDir(stored);
    } catch {
      // Storage unavailable (private mode) — keep the default.
    }
  }, []);
  const set = useCallback((d: LayoutDirection) => {
    setDir(d);
    try {
      window.localStorage.setItem(STORAGE_KEY, d);
    } catch {
      // Persistence is best-effort.
    }
  }, []);
  return [dir, set];
}
