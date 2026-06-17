// SSE subscription using fetch-event-source. It runs over fetch, so the browser
// attaches the HttpOnly `dagron_session` cookie same-origin automatically — no
// Authorization header needed (it also gives nicer reconnect/openWhenHidden
// behavior than native EventSource).

import { fetchEventSource } from "@microsoft/fetch-event-source";
import type { TaskEvent } from "@/types/dagron";

export interface SubscribeHandlers {
  onEvent: (ev: TaskEvent) => void;
  /// Server emits `resync` on broadcast lag — refetch the full graph.
  onResync: () => void;
  onStatus?: (s: "live" | "reconnecting" | "offline") => void;
}

/// Subscribe to a run's SSE stream. Returns an unsubscribe function.
export function subscribeRun(runId: string, h: SubscribeHandlers): () => void {
  const ctrl = new AbortController();

  fetchEventSource(`/api/runs/${encodeURIComponent(runId)}/stream`, {
    signal: ctrl.signal,
    credentials: "same-origin",
    openWhenHidden: true,
    onopen: async () => {
      h.onStatus?.("live");
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
      // Returning (not throwing) lets the library retry with backoff.
      h.onStatus?.("reconnecting");
      void err;
    },
    onclose: () => {
      h.onStatus?.("offline");
    },
  }).catch(() => {
    h.onStatus?.("offline");
  });

  return () => ctrl.abort();
}
