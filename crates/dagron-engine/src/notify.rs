//! Outbound run notifications: `notify.webhook` + `notify.slack`.
//!
//! Complements the `notify.git` forge feedback in `lib.rs`: where that posts a
//! commit status, these push the run outcome to operators — a generic JSON
//! webhook (PagerDuty/ops-bridge shaped) and a Slack incoming webhook. Both are
//! best-effort: a notification target being down never affects run execution.
//!
//! Fired for two event kinds:
//! * run finalization — `event` is the terminal status (`succeeded` / `failed` /
//!   `cancelled`);
//! * soft SLA breach — `event` is `deadline_exceeded` (the run keeps running).

use std::time::Duration;

use crate::db;
use dagron_core::dag::DagSpec;

/// Global notification defaults, written by dagron-api into the `ui_settings`
/// table (key `notifications`). Mirrors `routes/settings.rs` in dagron-api —
/// keep the field names in sync. Applied to every run *in addition to* the
/// spec's own `notify:` block; a spec target with the same URL wins so nothing
/// fires twice.
#[derive(Debug, Default, serde::Deserialize)]
struct GlobalNotify {
    #[serde(default)]
    slack_enabled: bool,
    #[serde(default)]
    slack_webhook_url: String,
    #[serde(default)]
    slack_on: Vec<String>,
    #[serde(default)]
    webhook_enabled: bool,
    #[serde(default)]
    webhook_url: String,
    #[serde(default)]
    webhook_on: Vec<String>,
}

/// Best-effort read of the dagron-api-owned settings row. The table only
/// exists on deployments running dagron-api against the same database — a
/// missing table (e.g. a standalone SQLite engine) is simply "no defaults".
async fn load_global(pool: &db::Pool) -> Option<GlobalNotify> {
    let raw = db::ui_setting(pool, "notifications").await.ok().flatten()?;
    serde_json::from_str(&raw).ok()
}

fn slack_payload(workflow: &str, run_id: &str, event: &str) -> serde_json::Value {
    let emoji = match event {
        "succeeded" => "✅",
        "failed" => "❌",
        "cancelled" => "⚪",
        "deadline_exceeded" => "⏰",
        _ => "ℹ️",
    };
    let text = if event == "deadline_exceeded" {
        format!("{emoji} dagron: *{workflow}* exceeded its SLA deadline (run `{run_id}` still running)")
    } else {
        format!("{emoji} dagron: *{workflow}* {event} (run `{run_id}`)")
    };
    serde_json::json!({ "text": text })
}

fn webhook_payload(workflow: &str, run_id: &str, event: &str) -> serde_json::Value {
    serde_json::json!({
        "event": event,
        "run_id": run_id,
        "workflow": workflow,
        "status": if event == "deadline_exceeded" { "running" } else { event },
        "at": chrono::Utc::now().to_rfc3339(),
    })
}

/// Send any configured webhook/slack notifications for `run_id`: the spec's
/// own `notify:` targets first, then the instance-wide defaults from
/// `ui_settings` (skipping any URL the spec already fired). `event` is the
/// terminal status or `deadline_exceeded`.
pub async fn notify_run_event(pool: &db::Pool, run_id: &str, event: &str) {
    // The spec is optional here: a run whose YAML no longer parses (or is
    // gone) still triggers the global defaults, under its stored name.
    let spec: Option<DagSpec> = match db::spec_for_run(pool, run_id).await {
        Ok(Some(yaml)) => serde_yaml::from_str(&yaml).ok(),
        _ => None,
    };
    let workflow = match &spec {
        Some(s) => s.name.clone(),
        None => db::workflow_name_for_run(pool, run_id)
            .await
            .ok()
            .flatten()
            .unwrap_or_else(|| run_id.to_string()),
    };

    // URLs already notified, so a global default matching a spec target
    // doesn't double-fire.
    let mut fired: Vec<String> = Vec::new();

    if let Some(s) = &spec {
        if let Some(notify) = &s.notify {
            let sub = |v: &str| dagron_core::expand::substitute(v, &s.parameters);

            if let Some(hook) = &notify.webhook {
                if webhook_fires(&hook.on, event) {
                    let url = sub(&hook.url);
                    post_json(&url, &webhook_payload(&workflow, run_id, event), run_id, "notify.webhook")
                        .await;
                    fired.push(url);
                }
            }
            if let Some(slack) = &notify.slack {
                if slack_fires(&slack.on, event) {
                    let url = sub(&slack.webhook_url);
                    post_json(&url, &slack_payload(&workflow, run_id, event), run_id, "notify.slack")
                        .await;
                    fired.push(url);
                }
            }
        }
    }

    // Instance-wide defaults (configured in the UI under Notifications).
    // Same per-target `on` semantics as the spec block.
    let Some(global) = load_global(pool).await else {
        return;
    };
    if global.webhook_enabled
        && !global.webhook_url.is_empty()
        && webhook_fires(&global.webhook_on, event)
        && !fired.iter().any(|u| u == &global.webhook_url)
    {
        post_json(
            &global.webhook_url,
            &webhook_payload(&workflow, run_id, event),
            run_id,
            "global notify.webhook",
        )
        .await;
    }
    if global.slack_enabled
        && !global.slack_webhook_url.is_empty()
        && slack_fires(&global.slack_on, event)
        && !fired.iter().any(|u| u == &global.slack_webhook_url)
    {
        post_json(
            &global.slack_webhook_url,
            &slack_payload(&workflow, run_id, event),
            run_id,
            "global notify.slack",
        )
        .await;
    }
}

/// Webhook default (empty `on`) = every event.
fn webhook_fires(on: &[String], event: &str) -> bool {
    on.is_empty() || on.iter().any(|e| e == event)
}

/// Slack default (empty `on`) = incidents only: failed + deadline_exceeded.
fn slack_fires(on: &[String], event: &str) -> bool {
    if on.is_empty() {
        matches!(event, "failed" | "deadline_exceeded")
    } else {
        on.iter().any(|e| e == event)
    }
}

/// One shared client for all notification posts — connection pool + TLS setup
/// happen once, not per event (reqwest's own recommendation). `OnceLock` keeps
/// the MSRV bar low (stable since 1.70).
fn client() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("building the notification http client cannot fail")
    })
}

async fn post_json(url: &str, payload: &serde_json::Value, run_id: &str, kind: &str) {
    if url.is_empty() || url.contains("{{") {
        tracing::warn!(%run_id, kind, "notification url did not resolve (missing parameter?) — skipping");
        return;
    }
    // Same scheme bar the UI-configured defaults are validated against
    // (settings.rs): plain http(s) only — no file://, gopher://, etc. via a
    // templated parameter.
    if !(url.starts_with("https://") || url.starts_with("http://")) {
        tracing::warn!(%run_id, kind, "notification url is not http(s) — skipping");
        return;
    }
    match client().post(url).json(payload).send().await {
        Ok(res) if !res.status().is_success() => {
            tracing::warn!(%run_id, kind, status = %res.status(), "notification endpoint returned non-2xx");
        }
        Ok(_) => tracing::debug!(%run_id, kind, "notification sent"),
        Err(e) => tracing::warn!(%run_id, kind, error = %e, "notification post failed"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn webhook_defaults_to_all_events() {
        assert!(webhook_fires(&[], "succeeded"));
        assert!(webhook_fires(&[], "deadline_exceeded"));
        assert!(!webhook_fires(&["failed".into()], "succeeded"));
        assert!(webhook_fires(&["failed".into()], "failed"));
    }

    #[test]
    fn slack_defaults_to_incidents_only() {
        assert!(!slack_fires(&[], "succeeded"));
        assert!(slack_fires(&[], "failed"));
        assert!(slack_fires(&[], "deadline_exceeded"));
        assert!(slack_fires(&["succeeded".into()], "succeeded"));
        assert!(!slack_fires(&["succeeded".into()], "failed"));
    }
}
