"use client";

// App-shell layout preferences: the sidebar rail, and the agent dock.
//
// Same shape as `live.ts` — one localStorage key per preference, read through
// `useSyncExternalStore` so every mounted consumer and every other tab stays in
// step. Chrome layout is an account-local preference, not server state: a
// console left open on a wall display wants the rail, the same account on a
// laptop wants the labels, and neither should tell the other what to do.

import { useSyncExternalStore } from "react";

const SIDE_KEY = "dagron.sidebarRail";
const DOCK_KEY = "dagron.agentDock";
const BLOCKS_KEY = "dagron.blocksRail";
const SIZE_KEY = "dagron.pageSize";
const CHANGE_EVT = "dagron:shell-prefs";

/// The last value written, per key, when the store refused it (private mode,
/// storage disabled). `read` prefers this: without it a failed write dispatched
/// an event whose subscribers re-read the store, got the old value back, and
/// left the operator unable to open a dock or leave a collapsed rail.
const volatilePrefs = new Map<string, boolean>();

function subscribe(cb: () => void): () => void {
  window.addEventListener("storage", cb); // other tabs
  window.addEventListener(CHANGE_EVT, cb); // this tab
  return () => {
    window.removeEventListener("storage", cb);
    window.removeEventListener(CHANGE_EVT, cb);
  };
}

function read(key: string, dflt: boolean): boolean {
  // Checked BEFORE the store, not just in the catch below: `setItem` can throw
  // on quota while `getItem` keeps answering, so a failed write leaves the
  // store readable AND stale. Reading it there would hand back the value the
  // operator just replaced. An entry here means the last write did not land,
  // which makes it the newer of the two.
  const pending = volatilePrefs.get(key);
  if (pending !== undefined) return pending;
  try {
    const v = localStorage.getItem(key);
    return v == null ? dflt : v === "on";
  } catch {
    return dflt; // storage disabled outright — nothing written, nothing stale
  }
}

function write(key: string, v: boolean) {
  // Recorded before the attempt, so a throw leaves the choice readable.
  volatilePrefs.set(key, v);
  try {
    localStorage.setItem(key, v ? "on" : "off");
    // Persisted: drop the fallback so another tab's value is not shadowed.
    volatilePrefs.delete(key);
  } catch {
    // Kept above; `read` answers from it until a write succeeds.
  }
  window.dispatchEvent(new Event(CHANGE_EVT));
}

/// Sidebar collapsed to an icon rail. Default off — a first-time operator
/// should see the labels, and discover the rail when they want the width.
export function useSidebarRail(): [boolean, (v: boolean) => void] {
  const rail = useSyncExternalStore(
    subscribe,
    () => read(SIDE_KEY, false),
    () => false, // SSR snapshot: expanded, so the exported HTML is the wide one
  );
  return [rail, (v: boolean) => write(SIDE_KEY, v)];
}

/// The agent dock on the right. Default off: it is a working surface someone
/// opts into, and it costs canvas width the run graph wants.
export function useAgentDock(): [boolean, (v: boolean) => void] {
  const open = useSyncExternalStore(
    subscribe,
    () => read(DOCK_KEY, false),
    () => false,
  );
  return [open, (v: boolean) => write(DOCK_KEY, v)];
}

/// Rows per page on the list views.
///
/// `GET /api/runs` and `/api/archive/runs` both cap `limit` at 500, and the
/// list asks for one extra row to detect a next page — so 200 is the largest
/// size offered rather than 500. 50 stays the default: it fills a screen
/// without asking the API for a page nobody scrolls to.
export const PAGE_SIZES = [25, 50, 100, 200] as const;
export type PageSize = (typeof PAGE_SIZES)[number];
const DEFAULT_PAGE_SIZE: PageSize = 50;

export function usePageSize(): [PageSize, (v: PageSize) => void] {
  const size = useSyncExternalStore(
    subscribe,
    () => {
      let raw: string | null = null;
      try {
        raw = localStorage.getItem(SIZE_KEY);
      } catch {
        return DEFAULT_PAGE_SIZE;
      }
      const n = Number(raw);
      // A hand-edited or stale value must not become an unbounded `limit`: the
      // API would reject it and the list would show an error instead of rows.
      return (PAGE_SIZES as readonly number[]).includes(n) ? (n as PageSize) : DEFAULT_PAGE_SIZE;
    },
    () => DEFAULT_PAGE_SIZE,
  );
  return [
    size,
    (v: PageSize) => {
      try {
        localStorage.setItem(SIZE_KEY, String(v));
      } catch {
        // the in-tab event below still moves mounted consumers
      }
      window.dispatchEvent(new Event(CHANGE_EVT));
    },
  ];
}

/// The editor's block palette, collapsed to a rail. Default off — the blocks
/// are the point of the editor for someone composing a first spec, and the
/// people who want the width back are the ones who no longer need them.
export function useBlocksRail(): [boolean, (v: boolean) => void] {
  const rail = useSyncExternalStore(
    subscribe,
    () => read(BLOCKS_KEY, false),
    () => false,
  );
  return [rail, (v: boolean) => write(BLOCKS_KEY, v)];
}
