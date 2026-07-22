"use client";

// Global "live updates" preference + event-driven refresh plumbing.
//
// The toggle is one account-local preference (localStorage) shared by every
// page: Runs, Workflows, Overview, and the run detail view all read the same
// key, so pausing live reads in one place pauses them everywhere. Cross-tab
// sync rides the native `storage` event; same-tab components sync via a
// custom window event.
//
// Live mode is event-driven, not polling: pages subscribe to one SSE stream
// and coalesce event bursts into a debounced refetch (with a max-wait so a
// sustained burst still repaints periodically). Paused mode costs zero
// background requests — data loads once and on manual refresh.

import { useEffect, useRef, useState, useSyncExternalStore } from "react";
import { subscribeEvents } from "@/lib/dagron-stream";

const KEY = "dagron.liveUpdates";
const CHANGE_EVT = "dagron:live-updates";

export type ConnStatus = "live" | "reconnecting" | "offline" | "paused";

/// Dot color for a connection status (matches the run-page status dot).
export function connColor(s: ConnStatus): string {
  if (s === "live") return "#2ea043";
  if (s === "reconnecting") return "#d29922";
  return "#6e7681";
}

function readPref(): boolean {
  try {
    return localStorage.getItem(KEY) !== "off"; // default ON
  } catch {
    return true;
  }
}

function subscribePref(cb: () => void): () => void {
  window.addEventListener("storage", cb); // other tabs
  window.addEventListener(CHANGE_EVT, cb); // this tab
  return () => {
    window.removeEventListener("storage", cb);
    window.removeEventListener(CHANGE_EVT, cb);
  };
}

/// The global live-updates preference. SSR-safe (server snapshot = ON);
/// setting it notifies every mounted consumer and other tabs.
export function useLiveUpdates(): [boolean, (v: boolean) => void] {
  const live = useSyncExternalStore(subscribePref, readPref, () => true);
  const setLive = (v: boolean) => {
    try {
      localStorage.setItem(KEY, v ? "on" : "off");
    } catch {
      // private mode etc. — the in-tab event still flips mounted consumers
    }
    window.dispatchEvent(new Event(CHANGE_EVT));
  };
  return [live, setLive];
}

/// Coalesce a burst of calls into one: trailing debounce of `debounceMs`, but
/// never delay past `maxWaitMs` from the first call of the burst, so a
/// sustained event storm still flushes periodically instead of starving.
function makeCoalescer(fn: () => void, debounceMs: number, maxWaitMs: number) {
  let timer: ReturnType<typeof setTimeout> | null = null;
  let burstStart = 0;
  const call = () => {
    const now = Date.now();
    if (timer) clearTimeout(timer);
    else burstStart = now;
    const wait = Math.min(debounceMs, Math.max(0, burstStart + maxWaitMs - now));
    timer = setTimeout(() => {
      timer = null;
      fn();
    }, wait);
  };
  call.cancel = () => {
    if (timer) clearTimeout(timer);
    timer = null;
  };
  return call;
}

/// Event-driven refresh for list pages. While `enabled`, subscribes to the
/// account-wide SSE stream and calls `refetch` on activity — debounced 400ms,
/// flushed at least every 2s during sustained bursts. While disabled, no
/// stream is held open and status reads "paused".
///
/// `refetch` is kept in a ref, so callers may pass a fresh closure every
/// render without resubscribing.
export function useLiveRefresh(enabled: boolean, refetch: () => void): ConnStatus {
  const [status, setStatus] = useState<ConnStatus>(enabled ? "offline" : "paused");
  const fnRef = useRef(refetch);
  fnRef.current = refetch;
  const wasPaused = useRef(false);

  useEffect(() => {
    if (!enabled) {
      setStatus("paused");
      wasPaused.current = true;
      return;
    }
    setStatus("offline");
    const kick = makeCoalescer(() => fnRef.current(), 400, 2000);
    let opened = false;
    const unsub = subscribeEvents({
      onEvent: kick,
      onResync: kick,
      onStatus: (s) => {
        setStatus(s);
        if (s !== "live") return;
        // Catch up on (re)connect — events may have been missed while the
        // stream was down or paused. Skip the very first open of a fresh
        // mount: the page's own initial load just ran.
        if (opened || wasPaused.current) kick();
        opened = true;
        wasPaused.current = false;
      },
    });
    return () => {
      unsub();
      kick.cancel();
    };
  }, [enabled]);

  return status;
}
