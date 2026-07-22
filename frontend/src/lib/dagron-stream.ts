// SSE subscription using fetch-event-source. It runs over fetch, so the browser
// attaches the HttpOnly `dagron_session` cookie same-origin automatically — no
// Authorization header needed (it also gives nicer reconnect behavior than
// native EventSource).
//
// Streams close while the tab is hidden (`openWhenHidden: false`): a background
// tab holds no connection and triggers no refetches. The library reopens on
// tab re-show, which fires `onopen` → callers refetch there to catch up.

import { EventStreamContentType, fetchEventSource } from "@microsoft/fetch-event-source";
import type { TaskEvent } from "@/types/dagron";

/// A response retrying can't heal (e.g. 401 expired session) — stop, don't
/// hammer the API; the UI lands on "offline".
class FatalStreamError extends Error {}

// Reconnect backoff bounds. fetch-event-source retries every ~1s by default,
// which would let N open tabs hammer the endpoint through a backend outage.
// Cap the growth and jitter it so tabs spread out instead of synchronizing.
const BACKOFF_BASE_MS = 1000;
const BACKOFF_CAP_MS = 30_000;

/// Parse an HTTP `Retry-After` header (delta-seconds or an HTTP date) to a
/// non-negative millisecond delay; null when absent/unparseable.
function parseRetryAfter(v: string | null): number | null {
  if (!v) return null;
  const secs = Number(v);
  if (Number.isFinite(secs)) return Math.max(0, secs * 1000);
  const at = Date.parse(v);
  return Number.isNaN(at) ? null : Math.max(0, at - Date.now());
}

export interface SubscribeHandlers {
  onEvent: (ev: TaskEvent) => void;
  /// Server emits `resync` on broadcast lag — refetch the full graph.
  onResync: () => void;
  onStatus?: (s: "live" | "reconnecting" | "offline") => void;
}

/// Subscribe to a run's SSE stream. Returns an unsubscribe function.
export function subscribeRun(runId: string, h: SubscribeHandlers): () => void {
  return subscribe(`/api/runs/${encodeURIComponent(runId)}/stream`, h);
}

/// Subscribe to the account-wide activity stream (every run's task events).
/// Feeds the list pages' live mode. Returns an unsubscribe function.
export function subscribeEvents(h: SubscribeHandlers): () => void {
  return subscribe(`/api/events/stream`, h);
}

function subscribe(url: string, h: SubscribeHandlers): () => void {
  const ctrl = new AbortController();

  // Per-subscription reconnect state, reset on every clean open.
  let attempt = 0;
  // Set from a 429's Retry-After so the next backoff honors the server's ask.
  let retryAfterMs: number | null = null;

  fetchEventSource(url, {
    signal: ctrl.signal,
    credentials: "same-origin",
    openWhenHidden: false,
    onopen: async (res) => {
      // Only a real event stream counts as live — a 401/5xx or an HTML error
      // page must not light the green dot. Thrown errors route to onerror.
      const ct = res.headers.get("content-type") ?? "";
      if (res.ok && ct.startsWith(EventStreamContentType)) {
        attempt = 0;
        retryAfterMs = null;
        h.onStatus?.("live");
        return;
      }
      if (res.status >= 400 && res.status < 500 && res.status !== 429) {
        throw new FatalStreamError(`stream rejected: ${res.status}`);
      }
      if (res.status === 429) {
        retryAfterMs = parseRetryAfter(res.headers.get("retry-after"));
      }
      throw new Error(`stream unavailable: ${res.status}`);
    },
    onmessage: (msg) => {
      if (msg.event === "resync") {
        h.onResync();
        return;
      }
      if (msg.data) {
        try {
          h.onEvent(JSON.parse(msg.data) as TaskEvent);
        } catch {
          // ignore malformed frame
        }
      }
    },
    onerror: (err) => {
      // Rethrowing kills the subscription (caught below → "offline"); returning
      // a number is the reconnect delay the library waits before retrying.
      if (err instanceof FatalStreamError) throw err;
      h.onStatus?.("reconnecting");
      // Capped exponential backoff with equal jitter: half fixed growth, half
      // random, so retries never dip below cap/2 yet tabs desynchronize. A 429
      // Retry-After takes precedence when the server sent a longer wait.
      const ceil = Math.min(BACKOFF_CAP_MS, BACKOFF_BASE_MS * 2 ** attempt);
      attempt += 1;
      const jittered = ceil / 2 + Math.random() * (ceil / 2);
      const delay = retryAfterMs != null ? Math.max(retryAfterMs, jittered) : jittered;
      retryAfterMs = null;
      return delay;
    },
    onclose: () => {
      h.onStatus?.("offline");
    },
  }).catch(() => {
    h.onStatus?.("offline");
  });

  return () => ctrl.abort();
}
