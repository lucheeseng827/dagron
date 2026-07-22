"use client";

// Instance-wide notification defaults: a Slack incoming webhook and/or a
// generic JSON webhook the engine fires for EVERY run's terminal status and
// SLA breach — on top of (never instead of) per-workflow `notify:` blocks.

import { useEffect, useState } from "react";
import { useToast } from "@/components/Toasts";
import {
  getMe,
  getNotificationSettings,
  saveNotificationSettings,
  testNotificationSettings,
  type Me,
} from "@/lib/dagron-api";
import { errMsg } from "@/lib/err";
import type { NotificationSettings, NotifyEvent } from "@/types/dagron";

const EVENTS: { id: NotifyEvent; label: string }[] = [
  { id: "succeeded", label: "Succeeded" },
  { id: "failed", label: "Failed" },
  { id: "cancelled", label: "Cancelled" },
  { id: "deadline_exceeded", label: "SLA deadline exceeded" },
];

const EMPTY: NotificationSettings = {
  slack_enabled: false,
  slack_webhook_url: "",
  slack_on: [],
  webhook_enabled: false,
  webhook_url: "",
  webhook_on: [],
};

export default function NotificationSettingsPage() {
  const toast = useToast();
  const [me, setMe] = useState<Me | null>(null);
  const [s, setS] = useState<NotificationSettings>(EMPTY);
  const [loaded, setLoaded] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [testResult, setTestResult] = useState<string | null>(null);

  const isAdmin = me?.groups?.includes("admin") ?? false;

  useEffect(() => {
    getMe().then(setMe).catch(() => {});
    getNotificationSettings()
      .then((data) => {
        setS({ ...EMPTY, ...data });
        setLoaded(true);
      })
      .catch((e) => setError(errMsg(e)));
  }, []);

  const patch = (p: Partial<NotificationSettings>) => setS((cur) => ({ ...cur, ...p }));

  const toggleEvent = (key: "slack_on" | "webhook_on", ev: NotifyEvent) =>
    setS((cur) => ({
      ...cur,
      [key]: cur[key].includes(ev) ? cur[key].filter((e) => e !== ev) : [...cur[key], ev],
    }));

  const onSave = async () => {
    setBusy(true);
    try {
      const saved = await saveNotificationSettings(s);
      setS({ ...EMPTY, ...saved });
      toast("Notification defaults saved");
    } catch (e) {
      toast(errMsg(e), "error");
    } finally {
      setBusy(false);
    }
  };

  const onTest = async () => {
    setBusy(true);
    setTestResult(null);
    try {
      const r = await testNotificationSettings(s);
      setTestResult(`Slack: ${r.slack} · Webhook: ${r.webhook}`);
    } catch (e) {
      toast(errMsg(e), "error");
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="dy-page" style={{ maxWidth: 760 }}>
      <div className="dy-pagehead">
        <div>
          <h1 className="dy-h1" style={{ marginBottom: 0 }}>
            Notifications
          </h1>
          <p className="dy-subtitle">
            Instance-wide defaults, fired by the engine for every run. Per-workflow{" "}
            <code className="mono">notify:</code> blocks still apply on top; a matching URL never
            fires twice.
          </p>
        </div>
      </div>
      {me && !isAdmin && (
        <div className="dy-card" style={{ borderColor: "var(--amber)" }}>
          <p style={{ margin: 0, color: "var(--amber)" }}>
            The admin group is required to view or change notification defaults (the stored webhook
            URLs are secrets, and changing them reroutes every run&apos;s notifications).
          </p>
        </div>
      )}
      {error && isAdmin && <p style={{ color: "var(--red)" }}>{error}</p>}

      {isAdmin && loaded && (
        <>
          <TargetCard
            title="Slack"
            hint="Incoming-webhook URL (the channel is fixed by the webhook). Default events: failures + SLA breaches only."
            enabled={s.slack_enabled}
            onEnabled={(v) => patch({ slack_enabled: v })}
            url={s.slack_webhook_url}
            onUrl={(v) => patch({ slack_webhook_url: v })}
            urlPlaceholder="https://hooks.slack.com/services/…"
            on={s.slack_on}
            onToggleEvent={(ev) => toggleEvent("slack_on", ev)}
            defaultNote="failed + SLA breach"
          />
          <TargetCard
            title="Webhook"
            hint="POSTs { event, run_id, workflow, status, at } as JSON. Default events: everything."
            enabled={s.webhook_enabled}
            onEnabled={(v) => patch({ webhook_enabled: v })}
            url={s.webhook_url}
            onUrl={(v) => patch({ webhook_url: v })}
            urlPlaceholder="https://ops.example.com/hooks/dagron"
            on={s.webhook_on}
            onToggleEvent={(ev) => toggleEvent("webhook_on", ev)}
            defaultNote="all events"
          />

          <div style={{ display: "flex", gap: 10, alignItems: "center", marginTop: 4 }}>
            <button onClick={onSave} disabled={busy} className="dy-btn dy-btn-primary">
              Save defaults
            </button>
            <button
              onClick={onTest}
              disabled={busy || (!s.slack_enabled && !s.webhook_enabled)}
              className="dy-btn"
              title="Send a test message to each enabled target (uses the values on screen, saved or not)"
            >
              Send test
            </button>
            {testResult && (
              <span className="mono" style={{ fontSize: 12.5, color: "var(--muted)" }}>
                {testResult}
              </span>
            )}
          </div>

          <p style={{ color: "var(--dim)", fontSize: 12.5, marginTop: 18, lineHeight: 1.6 }}>
            Notifications are sent by the scheduler engine when a run finalizes (succeeded / failed /
            cancelled) or exceeds its soft <code className="mono">deadline</code>. They are
            best-effort — an unreachable target never affects run execution. Workflow-specific
            routing belongs in the workflow&apos;s own <code className="mono">notify:</code> block
            (see the editor&apos;s snippet palette).
          </p>
        </>
      )}
    </div>
  );
}

function TargetCard({
  title,
  hint,
  enabled,
  onEnabled,
  url,
  onUrl,
  urlPlaceholder,
  on,
  onToggleEvent,
  defaultNote,
}: {
  title: string;
  hint: string;
  enabled: boolean;
  onEnabled: (v: boolean) => void;
  url: string;
  onUrl: (v: string) => void;
  urlPlaceholder: string;
  on: NotifyEvent[];
  onToggleEvent: (ev: NotifyEvent) => void;
  defaultNote: string;
}) {
  // Webhook URLs are bearer-like secrets (especially Slack's) — masked by
  // default with an explicit reveal, like the password field on the Users page.
  const [reveal, setReveal] = useState(false);
  return (
    <div className="dy-card" style={{ marginBottom: 14, opacity: enabled ? 1 : 0.75 }}>
      <label style={{ display: "flex", alignItems: "center", gap: 10, cursor: "pointer" }}>
        <input type="checkbox" checked={enabled} onChange={(e) => onEnabled(e.target.checked)} />
        <strong>{title}</strong>
        <span style={{ fontSize: 12.5, color: "var(--muted)" }}>{hint}</span>
      </label>
      {enabled && (
        <div style={{ marginTop: 12 }}>
          <div style={{ display: "flex", gap: 8 }}>
            <input
              value={url}
              onChange={(e) => onUrl(e.target.value)}
              placeholder={urlPlaceholder}
              type={reveal ? "text" : "password"}
              autoComplete="off"
              className="mono"
              style={{
                flex: 1,
                background: "var(--bg)",
                color: "var(--fg)",
                border: "1px solid var(--border)",
                borderRadius: 8,
                padding: "8px 12px",
              }}
            />
            <button type="button" className="dy-btn" onClick={() => setReveal((r) => !r)}>
              {reveal ? "Hide" : "Show"}
            </button>
          </div>
          <div style={{ display: "flex", gap: 12, alignItems: "center", marginTop: 10, flexWrap: "wrap" }}>
            <span style={{ fontSize: 12, color: "var(--dim)" }}>
              Events (none checked = default: {defaultNote}):
            </span>
            {EVENTS.map((ev) => (
              <label key={ev.id} style={{ display: "inline-flex", alignItems: "center", gap: 5, fontSize: 12.5, color: "var(--muted)", cursor: "pointer" }}>
                <input type="checkbox" checked={on.includes(ev.id)} onChange={() => onToggleEvent(ev.id)} />
                {ev.label}
              </label>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
