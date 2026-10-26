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
//!
//! A webhook's post is a CloudEvent in the binary content mode: the body
//! is the JSON it always was, and the event's attributes ride in `ce-*`
//! headers, its type `dev.dagron.notify.<event>`. One occurrence has one id, so
//! a spec's target and the instance's default hear the same event.

use std::time::Duration;

use crate::db;
use dagron_core::cloudevents;
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

/// The CloudEvent a webhook's post is: `id` names the occurrence, the same
/// for each target it goes to; its time is the payload's `at`.
fn webhook_event(id: &str, run_id: &str, event: &str, payload: &serde_json::Value) -> cloudevents::Event {
    cloudevents::Event {
        id: id.to_string(),
        source: cloudevents::source_from_env(),
        kind: format!("notify.{event}"),
        subject: Some(run_id.to_string()),
        time: payload["at"].as_str().map(str::to_string),
    }
}

/// The run's spec as its notifications see it: the stored YAML, with the
/// parameters the run was created with folded over the defaults its
/// `parameters:` block declares. So a template in a `notify:` target resolves
/// to what the run's tasks saw, including a value passed at submit
/// (`sha: "{{ commit_sha }}"`). A run from before parameters were stored
/// keeps the defaults alone, which is all it ever had.
///
/// `None` when the run is gone or its YAML no longer parses.
pub(crate) async fn run_spec(pool: &db::Pool, run_id: &str) -> Option<DagSpec> {
    let yaml = db::spec_for_run(pool, run_id).await.ok().flatten()?;
    let mut spec: DagSpec = serde_yaml::from_str(&yaml).ok()?;
    match db::run_parameters(pool, run_id).await {
        Ok(Some(stored)) => spec.parameters.extend(stored),
        Ok(None) => {}
        Err(e) => tracing::warn!(
            error = %e, %run_id,
            "could not read the run's parameters; its notifications resolve against the spec's defaults"
        ),
    }
    Some(spec)
}

/// Send any configured webhook/slack notifications for `run_id`: the spec's
/// own `notify:` targets first, then the instance-wide defaults from
/// `ui_settings` (skipping any URL the spec already fired). `event` is the
/// terminal status or `deadline_exceeded`.
pub async fn notify_run_event(pool: &db::Pool, run_id: &str, event: &str) {
    // The spec is optional here: a run whose YAML no longer parses (or is
    // gone) still triggers the global defaults, under its stored name.
    let spec = run_spec(pool, run_id).await;
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
    // The occurrence's CloudEvents id, one for every target it goes to.
    let event_id = uuid::Uuid::new_v4().to_string();

    if let Some(s) = &spec {
        if let Some(notify) = &s.notify {
            let sub = |v: &str| dagron_core::expand::substitute(v, &s.parameters);

            if let Some(hook) = &notify.webhook {
                if webhook_fires(&hook.on, event) {
                    let url = sub(&hook.url);
                    let payload = webhook_payload(&workflow, run_id, event);
                    let ce = webhook_event(&event_id, run_id, event, &payload);
                    post_json(&url, &payload, Some(&ce), run_id, "notify.webhook").await;
                    fired.push(url);
                }
            }
            if let Some(slack) = &notify.slack {
                if slack_fires(&slack.on, event) {
                    let url = sub(&slack.webhook_url);
                    post_json(&url, &slack_payload(&workflow, run_id, event), None, run_id, "notify.slack")
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
        let payload = webhook_payload(&workflow, run_id, event);
        let ce = webhook_event(&event_id, run_id, event, &payload);
        post_json(&global.webhook_url, &payload, Some(&ce), run_id, "global notify.webhook").await;
    }
    if global.slack_enabled
        && !global.slack_webhook_url.is_empty()
        && slack_fires(&global.slack_on, event)
        && !fired.iter().any(|u| u == &global.slack_webhook_url)
    {
        post_json(
            &global.slack_webhook_url,
            &slack_payload(&workflow, run_id, event),
            None,
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

/// Post `payload` to `url`, as the CloudEvent `ce` when it is one (a webhook;
/// Slack's incoming webhooks take their own shape and nothing else).
async fn post_json(
    url: &str,
    payload: &serde_json::Value,
    ce: Option<&cloudevents::Event>,
    run_id: &str,
    kind: &str,
) {
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
    let req = match ce {
        Some(ce) => {
            let (headers, body) = ce.encode(cloudevents::Mode::Binary, &payload.to_string());
            headers
                .into_iter()
                .fold(client().post(url), |req, (name, value)| req.header(name, value))
                .body(body)
        }
        None => client().post(url).json(payload),
    };
    match req.send().await {
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
    fn a_webhook_post_is_a_cloudevent_named_for_its_notification() {
        let payload = webhook_payload("nightly", "run-9", "failed");
        let ce = webhook_event("ev-1", "run-9", "failed", &payload);
        assert_eq!(ce.event_type(), "dev.dagron.notify.failed");
        assert_eq!(ce.subject.as_deref(), Some("run-9"));
        assert_eq!(ce.time.as_deref(), payload["at"].as_str(), "its time is the payload's");
        let (headers, body) = ce.encode(cloudevents::Mode::Binary, &payload.to_string());
        assert_eq!(serde_json::from_str::<serde_json::Value>(&body).unwrap(), payload);
        assert!(headers.iter().any(|(k, v)| *k == "ce-id" && v == "ev-1"));
    }

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

    const SHA: &str = "4b4708e725f29891d22b4c96af9f0a403ff51c7c";

    /// The gap this closes: a caller submits a run with the commit as a
    /// parameter, and the status is resolved when the run ends, from the
    /// stored YAML, whose `parameters:` block holds only the defaults. Before
    /// runs kept their parameters, the SHA resolved to the empty default and
    /// the status was skipped.
    #[tokio::test]
    async fn a_sha_passed_at_submit_reaches_the_commit_status() {
        let path =
            std::env::temp_dir().join(format!("module54_notify_{}.db", uuid::Uuid::new_v4()));
        let pool = db::init_pool(path.to_str().unwrap()).await.unwrap();
        let yaml = "name: etl\n\
                    parameters:\n  commit_sha: \"\"\n  hook: \"https://hooks.example/default\"\n\
                    notify:\n  git:\n    provider: github\n    repo: acme/etl\n    sha: \"{{ commit_sha }}\"\n\
                    tasks:\n- name: a\n  command: [\"true\"]\n";
        let params =
            std::collections::BTreeMap::from([("commit_sha".to_string(), SHA.to_string())]);
        let dag = dagron_core::dag::DagGraph::from_yaml_with_params(yaml, &params).unwrap();
        let run_id = db::create_run(&pool, &dag, yaml).await.unwrap();

        let spec = run_spec(&pool, &run_id).await.expect("the run's spec");
        let target = crate::git_target(&spec, &run_id, "succeeded").expect("the SHA resolved");
        assert_eq!(target.sha, SHA);
        // A default the caller did not override is still there.
        assert_eq!(spec.parameters["hook"], "https://hooks.example/default");

        // A run from before parameters were stored resolves against the
        // defaults, as it always did: here no SHA, so no status.
        sqlx::query("UPDATE workflow_runs SET parameters = NULL WHERE id = ?")
            .bind(&run_id)
            .execute(&pool)
            .await
            .unwrap();
        let spec = run_spec(&pool, &run_id).await.unwrap();
        assert!(crate::git_target(&spec, &run_id, "succeeded").is_none());

        assert!(run_spec(&pool, "no-such-run").await.is_none());
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }
}
