//! CDEvents: a run's end, sent as the CD Foundation's CI/CD
//! events, so that a tool that reads CDEvents (a dashboard of delivery
//! metrics, an orchestrator that waits on a pipeline) sees dagron's runs
//! without an adapter.
//!
//! With `DAGRON_CDEVENTS_URL` set, each run's end is a part of its end-of-run
//! work ([`crate::run_end`], the outbox's `engine.cdevents` sink): a
//! `dev.cdevents.taskrun.finished.0.3.0` for each of its tasks that ran, in
//! the order they finished, then a `dev.cdevents.pipelinerun.finished.0.3.0`
//! for the run, each with the time it happened. CDEvents spec v0.5.1, whose
//! schemas the tests check every event against (`tests/cdevents/`).
//!
//! Each is a CloudEvent in HTTP binary mode (`ce-*` headers, the CDEvent as
//! the body), its attributes as the spec's CloudEvents binding sets them:
//! `id`, `source`, `type` and `time` the context's, `subject` the
//! subject's id. The task runs' are POSTed [`IN_FLIGHT`] at a time, and the
//! pipeline run's once they all were, so that it is the last a receiver is
//! told. Any that is not answered 2xx fails the part, which is tried again
//! with all of them: an event's id is its end's outbox row and its place in
//! it, so a receiver that has one already can tell. A run of many tasks has
//! the time its events take ([`budget`]), and a lease to match
//! ([`LEASE_SECS`]), where the engine's other parts have a minute.
//!
//! Not sent: the events of a run starting or queueing, or of a task
//! starting, which would need the outbox to carry them as they happen.

use std::time::Duration;

use dagron_core::models::{RunStatus, TaskStatus};
use serde_json::{json, Value};

use crate::db;

/// Task runs' events in flight at once.
pub(crate) const IN_FLIGHT: usize = 16;
/// The longest a run's events may take, however many tasks it has.
pub(crate) const MAX_BUDGET: Duration = Duration::from_secs(30 * 60);
/// The lease on a claimed end of this part: past [`MAX_BUDGET`], so that
/// no other replica sends a run's events while they are being sent here.
pub(crate) const LEASE_SECS: i64 = 35 * 60;

/// How long a run's `events` events may take to send: a minute, and a
/// tenth of a second for each, to [`MAX_BUDGET`]. Sent [`IN_FLIGHT`] at a
/// time, that is room for a receiver that answers each in over a second.
pub(crate) fn budget(events: usize) -> Duration {
    (Duration::from_secs(60) + Duration::from_millis(100) * events as u32).min(MAX_BUDGET)
}

/// The CDEvents specification version the events are of.
pub(crate) const SPEC_VERSION: &str = "0.5.1";
const PIPELINE_RUN_FINISHED: &str = "dev.cdevents.pipelinerun.finished.0.3.0";
const TASK_RUN_FINISHED: &str = "dev.cdevents.taskrun.finished.0.3.0";

/// Where the events go, and who they are from.
pub(crate) struct CdEvents {
    http: reqwest::Client,
    url: String,
    source: String,
}

impl CdEvents {
    /// `DAGRON_CDEVENTS_URL` (unset: none), and `DAGRON_CDEVENTS_SOURCE`, the
    /// events' `source`, a URI reference (default `dagron`).
    pub(crate) fn from_env() -> Option<Self> {
        let url = std::env::var("DAGRON_CDEVENTS_URL")
            .ok()
            .filter(|u| !u.trim().is_empty())?;
        let source = std::env::var("DAGRON_CDEVENTS_SOURCE")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "dagron".to_string());
        Some(Self::new(url, source))
    }

    pub(crate) fn new(url: String, source: String) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("reqwest client with a timeout always builds"),
            url,
            source,
        }
    }

    /// Send `run_id`'s end, of the outbox row `end`. `Err` is tried again.
    pub(crate) async fn send_run_end(
        &self,
        pool: &db::Pool,
        end: &str,
        run_id: &str,
    ) -> Result<(), String> {
        let Some(run) = db::get_run(pool, run_id)
            .await
            .map_err(|e| format!("reading the run: {e:#}"))?
        else {
            return Ok(());
        };
        let tasks = db::list_tasks(pool, run_id)
            .await
            .map_err(|e| format!("reading the run's tasks: {e:#}"))?;
        let name = db::workflow_name_for_run(pool, run_id)
            .await
            .ok()
            .flatten()
            .unwrap_or_else(|| run_id.to_string());
        let run = RunFacts {
            id: run.id,
            name,
            status: run.status,
            finished_at: run
                .finished_at
                .unwrap_or_else(|| chrono::Utc::now().to_rfc3339()),
        };
        let tasks: Vec<TaskFacts> = tasks
            .into_iter()
            // A task cancelled before it began has a `finished_at` too (a
            // cancel stamps every task it ends), but it never ran: no
            // attempt of it was claimed. A parked one that finished (a
            // sensor, an approval) took no attempt either, and did run.
            .filter(|t| !(t.status == TaskStatus::Cancelled && t.attempt == 0))
            .filter_map(|t| {
                Some(TaskFacts {
                    finished_at: t.finished_at?,
                    id: t.id,
                    name: t.name,
                    status: t.status,
                })
            })
            .collect();
        let mut events = run_events(&self.source, end, &run, &tasks);
        let budget = budget(events.len());
        let run_event = events.pop().expect("a run's end has its own event");
        let sent = async {
            use futures_util::stream::{self, StreamExt, TryStreamExt};
            stream::iter(events.iter().map(Ok::<_, String>))
                .try_for_each_concurrent(IN_FLIGHT, |event| self.post(event))
                .await?;
            self.post(&run_event).await
        };
        tokio::time::timeout(budget, sent)
            .await
            .unwrap_or_else(|_| {
                Err(format!(
                    "the run's {} CDEvents took longer than {}s",
                    tasks.len() + 1,
                    budget.as_secs()
                ))
            })
    }

    async fn post(&self, event: &Value) -> Result<(), String> {
        let context = &event["context"];
        let header = |k: &str| context[k].as_str().unwrap_or_default().to_string();
        let answer = self
            .http
            .post(&self.url)
            .header("ce-specversion", "1.0")
            .header("ce-id", header("id"))
            .header("ce-source", header("source"))
            .header("ce-type", header("type"))
            .header(
                "ce-subject",
                event["subject"]["id"].as_str().unwrap_or_default(),
            )
            .header("ce-time", header("timestamp"))
            .header("content-type", "application/json")
            .body(event.to_string())
            .send()
            .await
            .map_err(|e| format!("sending a CDEvent: {e}"))?;
        if answer.status().is_success() {
            Ok(())
        } else {
            Err(format!("a CDEvent was answered HTTP {}", answer.status()))
        }
    }
}

/// What a run's events say of it.
#[derive(Debug, Clone)]
pub(crate) struct RunFacts {
    pub id: String,
    pub name: String,
    pub status: RunStatus,
    pub finished_at: String,
}

/// What a task's event says of it: one that has finished.
#[derive(Debug, Clone)]
pub(crate) struct TaskFacts {
    pub id: String,
    pub name: String,
    pub status: TaskStatus,
    pub finished_at: String,
}

/// The CDEvents of a run's end, of the outbox row `end`: a task run's for
/// each task that ran, in the order they finished, then the pipeline run's.
/// A task skipped or never begun has none: it did not run.
pub(crate) fn run_events(
    source: &str,
    end: &str,
    run: &RunFacts,
    tasks: &[TaskFacts],
) -> Vec<Value> {
    let mut ran: Vec<&TaskFacts> = tasks
        .iter()
        .filter(|t| task_outcome(&t.status).is_some())
        .collect();
    ran.sort_by(|a, b| a.finished_at.cmp(&b.finished_at).then(a.name.cmp(&b.name)));
    let context = |n: usize, kind: &str, at: &str| {
        json!({
            "specversion": SPEC_VERSION,
            "id": format!("{end}/{n}"),
            "source": source,
            "type": kind,
            "timestamp": at,
        })
    };
    let mut out = Vec::with_capacity(ran.len() + 1);
    for (n, t) in ran.iter().enumerate() {
        let outcome = task_outcome(&t.status).expect("only tasks that ran");
        let mut content = json!({
            "taskName": t.name,
            "pipelineRun": { "id": run.id, "source": source },
            "outcome": outcome,
        });
        if outcome == "failure" {
            content["errors"] = json!(format!("{} failed", t.name));
        }
        out.push(json!({
            "context": context(n, TASK_RUN_FINISHED, &t.finished_at),
            "subject": { "id": t.id, "source": source, "content": content },
        }));
    }
    let outcome = match run.status {
        RunStatus::Succeeded => "success",
        RunStatus::Cancelled => "cancel",
        _ => "failure",
    };
    let mut content = json!({ "pipelineName": run.name, "outcome": outcome });
    if outcome == "failure" {
        let failed: Vec<&str> = ran
            .iter()
            .filter(|t| t.status == TaskStatus::Failed)
            .map(|t| t.name.as_str())
            .collect();
        content["errors"] = json!(if failed.is_empty() {
            "the run failed".to_string()
        } else {
            format!("failed: {}", failed.join(", "))
        });
    }
    out.push(json!({
        "context": context(ran.len(), PIPELINE_RUN_FINISHED, &run.finished_at),
        "subject": { "id": run.id, "source": source, "content": content },
    }));
    out
}

/// A finished task's CDEvents outcome; `None` for one that did not run.
fn task_outcome(status: &TaskStatus) -> Option<&'static str> {
    match status {
        TaskStatus::Succeeded => Some("success"),
        TaskStatus::Failed => Some("failure"),
        TaskStatus::Cancelled => Some("cancel"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    const PIPELINE_RUN_SCHEMA: &str = include_str!("../tests/cdevents/pipelinerunfinished.json");
    const TASK_RUN_SCHEMA: &str = include_str!("../tests/cdevents/taskrunfinished.json");

    /// Check `value` against `schema`, with the JSON Schema the CDEvents
    /// schemas use: `type`, `properties`, `required`, `additionalProperties:
    /// false`, `enum`, `minLength`, `format` (`date-time`, `uri`,
    /// `uri-reference`) and `oneOf`. A `$ref` the instance reaches is refused,
    /// as the events carry no `links`. Returns what is wrong, by path.
    fn check(schema: &Value, value: &Value, at: &str, errors: &mut Vec<String>) {
        if schema.get("$ref").is_some() {
            errors.push(format!(
                "{at}: reaches a $ref, which this check does not follow"
            ));
            return;
        }
        if let Some(t) = schema.get("type").and_then(Value::as_str) {
            let ok = match t {
                "object" => value.is_object(),
                "string" => value.is_string(),
                "array" => value.is_array(),
                other => panic!("a type the check does not know: {other}"),
            };
            if !ok {
                errors.push(format!("{at}: not a {t}"));
                return;
            }
        }
        if let Some(options) = schema.get("enum").and_then(Value::as_array) {
            if !options.contains(value) {
                errors.push(format!("{at}: {value} is none of {options:?}"));
            }
        }
        if let (Some(min), Some(s)) = (
            schema.get("minLength").and_then(Value::as_u64),
            value.as_str(),
        ) {
            if (s.chars().count() as u64) < min {
                errors.push(format!("{at}: shorter than {min}"));
            }
        }
        if let (Some(format), Some(s)) =
            (schema.get("format").and_then(Value::as_str), value.as_str())
        {
            let ok = match format {
                "date-time" => chrono::DateTime::parse_from_rfc3339(s).is_ok(),
                "uri" => reqwest::Url::parse(s).is_ok(),
                "uri-reference" => !s.contains(char::is_whitespace),
                other => panic!("a format the check does not know: {other}"),
            };
            if !ok {
                errors.push(format!("{at}: not a {format}: {s}"));
            }
        }
        if let Some(options) = schema.get("oneOf").and_then(Value::as_array) {
            let fits = options
                .iter()
                .filter(|o| {
                    let mut e = Vec::new();
                    check(o, value, at, &mut e);
                    e.is_empty()
                })
                .count();
            if fits != 1 {
                errors.push(format!("{at}: fits {fits} of oneOf"));
            }
        }
        let Some(object) = value.as_object() else {
            return;
        };
        let properties = schema.get("properties").and_then(Value::as_object);
        for required in schema
            .get("required")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let key = required.as_str().unwrap();
            if !object.contains_key(key) {
                errors.push(format!("{at}: no {key}"));
            }
        }
        for (key, v) in object {
            match properties.and_then(|p| p.get(key)) {
                Some(s) => check(s, v, &format!("{at}.{key}"), errors),
                None if schema.get("additionalProperties") == Some(&Value::Bool(false)) => {
                    errors.push(format!("{at}: {key} is not allowed"))
                }
                None => {}
            }
        }
    }

    fn valid(event: &Value) -> Vec<String> {
        let schema = match event["context"]["type"].as_str() {
            Some(PIPELINE_RUN_FINISHED) => PIPELINE_RUN_SCHEMA,
            Some(TASK_RUN_FINISHED) => TASK_RUN_SCHEMA,
            other => return vec![format!("no schema for {other:?}")],
        };
        let mut errors = Vec::new();
        check(
            &serde_json::from_str(schema).unwrap(),
            event,
            "$",
            &mut errors,
        );
        errors
    }

    fn task(id: &str, status: TaskStatus, at: &str) -> TaskFacts {
        TaskFacts {
            id: format!("task-{id}"),
            name: id.to_string(),
            status,
            finished_at: at.to_string(),
        }
    }

    fn run(status: RunStatus) -> RunFacts {
        RunFacts {
            id: "run-1".into(),
            name: "ci".into(),
            status,
            finished_at: "2026-10-19T10:00:09.5+00:00".into(),
        }
    }

    /// Every event of a run's end, of each outcome, is valid against the
    /// v0.5.1 schemas; and the check catches what is not.
    #[test]
    fn every_event_validates_against_the_v0_5_1_schemas() {
        let tasks = [
            task("build", TaskStatus::Succeeded, "2026-10-19T10:00:03+00:00"),
            task("test", TaskStatus::Failed, "2026-10-19T10:00:05+00:00"),
            task("deploy", TaskStatus::Cancelled, "2026-10-19T10:00:08+00:00"),
        ];
        for status in [
            RunStatus::Succeeded,
            RunStatus::Failed,
            RunStatus::Cancelled,
        ] {
            for event in run_events("https://ci.example/dagron", "e-1", &run(status), &tasks) {
                assert_eq!(valid(&event), Vec::<String>::new(), "{event}");
            }
        }

        let mut bad = run_events("dagron", "e-1", &run(RunStatus::Failed), &[])
            .pop()
            .unwrap();
        bad["subject"]["content"]["outcome"] = json!("failed");
        bad["context"]["version"] = json!("0.5.0");
        bad["context"].as_object_mut().unwrap().remove("timestamp");
        let errors = valid(&bad);
        assert_eq!(errors.len(), 3, "{errors:?}");
    }

    /// A receiver that answers every POST `202` and keeps each one's `ce-*`
    /// headers and body, in arrival order.
    async fn receiver() -> (String, Arc<Mutex<Vec<(Vec<(String, String)>, Value)>>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/events", listener.local_addr().unwrap());
        let got = Arc::new(Mutex::new(Vec::new()));
        let seen = got.clone();
        tokio::spawn(async move {
            while let Ok((mut conn, _)) = listener.accept().await {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 8192];
                let (head, body) = loop {
                    match conn.read(&mut chunk).await {
                        Ok(0) | Err(_) => break (String::new(), Vec::new()),
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    }
                    let Some(at) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
                        continue;
                    };
                    let head = String::from_utf8_lossy(&buf[..at]).to_string();
                    let len: usize = head
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse().unwrap())
                        })
                        .unwrap_or(0);
                    if buf.len() >= at + 4 + len {
                        break (head, buf[at + 4..at + 4 + len].to_vec());
                    }
                };
                let headers = head
                    .lines()
                    .skip(1)
                    .filter_map(|l| l.split_once(':'))
                    .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
                    .filter(|(k, _)| k.starts_with("ce-") || k == "content-type")
                    .collect();
                if let Ok(body) = serde_json::from_slice(&body) {
                    seen.lock().unwrap().push((headers, body));
                }
                let _ = conn
                    .write_all(
                        b"HTTP/1.1 202 Accepted\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
                    )
                    .await;
            }
        });
        (url, got)
    }

    fn got_now(
        got: &Arc<Mutex<Vec<(Vec<(String, String)>, Value)>>>,
    ) -> Vec<(Vec<(String, String)>, Value)> {
        got.lock().unwrap().clone()
    }

    /// A run of many tasks is sent whole, its tasks' events side by side and
    /// its own last.
    #[tokio::test]
    async fn a_run_of_many_tasks_is_sent_whole_and_its_own_event_last() {
        let (url, got) = receiver().await;
        let cd = CdEvents::new(url, "dagron".into());
        let path =
            std::env::temp_dir().join(format!("module54_cdevents_{}.db", uuid::Uuid::new_v4()));
        let pool = db::init_pool(path.to_str().unwrap()).await.unwrap();
        let mut yaml = String::from("name: wide\ntasks:\n");
        for i in 0..200 {
            yaml.push_str(&format!("  - name: t{i:03}\n    command: [\"true\"]\n"));
        }
        let dag = crate::dag::DagGraph::from_yaml(&yaml).unwrap();
        let run_id = db::create_run(&pool, &dag, &yaml).await.unwrap();
        db::advance_ready_tasks(&pool).await.unwrap();
        for t in db::claim_ready(&pool, "w", 500).await.unwrap() {
            assert!(
                db::mark_task_succeeded(&pool, &t.id, "w", t.version + 1, None)
                    .await
                    .unwrap()
            );
        }
        assert_eq!(db::reap_completed_runs(&pool).await.unwrap().len(), 1);

        cd.send_run_end(&pool, "e-wide", &run_id).await.unwrap();
        let got = got_now(&got);
        assert_eq!(got.len(), 201);
        assert_eq!(
            got[200].1["context"]["type"], PIPELINE_RUN_FINISHED,
            "the run's own is the last"
        );
        let mut ids: Vec<&str> = got
            .iter()
            .map(|(_, e)| e["context"]["id"].as_str().unwrap())
            .collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 201, "each once");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A run that ends is sent, by the end-of-run part, as its tasks' and
    /// its own CDEvents: CloudEvents in binary mode whose headers are the
    /// events' context, each valid against the schemas; and once.
    #[tokio::test]
    async fn a_run_that_ends_is_sent_as_cdevents_once() {
        let path =
            std::env::temp_dir().join(format!("module54_cdevents_{}.db", uuid::Uuid::new_v4()));
        let pool = db::init_pool(path.to_str().unwrap()).await.unwrap();
        let (url, got_all) = receiver().await;
        let part = crate::run_end::Part::CdEvents(Arc::new(CdEvents::new(
            url,
            "https://ci.example/dagron".into(),
        )));
        crate::run_end::register(&pool, std::slice::from_ref(&part))
            .await
            .unwrap();

        let yaml = "name: ci\ntasks:\n  - name: build\n    command: [\"true\"]\n  - name: test\n    command: [\"true\"]\n";
        let dag = crate::dag::DagGraph::from_yaml(yaml).unwrap();
        let run_id = db::create_run(&pool, &dag, yaml).await.unwrap();
        db::advance_ready_tasks(&pool).await.unwrap();
        for t in db::claim_ready(&pool, "w", 10).await.unwrap() {
            assert!(
                db::mark_task_succeeded(&pool, &t.id, "w", t.version + 1, None)
                    .await
                    .unwrap()
            );
        }
        assert_eq!(db::reap_completed_runs(&pool).await.unwrap().len(), 1);

        assert_eq!(crate::run_end::drain_once(&pool, &part).await.unwrap(), 1);
        assert_eq!(
            crate::run_end::drain_once(&pool, &part).await.unwrap(),
            0,
            "once"
        );
        let got = got_now(&got_all);
        let types: Vec<&str> = got
            .iter()
            .map(|(_, e)| e["context"]["type"].as_str().unwrap())
            .collect();
        assert_eq!(
            types,
            [TASK_RUN_FINISHED, TASK_RUN_FINISHED, PIPELINE_RUN_FINISHED]
        );
        for (headers, event) in &got {
            assert_eq!(valid(event), Vec::<String>::new(), "{event}");
            let header = |k: &str| {
                headers
                    .iter()
                    .find(|(h, _)| h == k)
                    .map(|(_, v)| v.as_str())
            };
            assert_eq!(header("ce-specversion"), Some("1.0"));
            assert_eq!(header("ce-id"), event["context"]["id"].as_str());
            assert_eq!(header("ce-type"), event["context"]["type"].as_str());
            assert_eq!(header("ce-subject"), event["subject"]["id"].as_str());
            assert_eq!(header("ce-time"), event["context"]["timestamp"].as_str());
            assert_eq!(header("ce-source"), Some("https://ci.example/dagron"));
            assert_eq!(header("content-type"), Some("application/json"));
        }
        let finished = &got[2].1["subject"];
        assert_eq!(finished["id"], run_id.as_str());
        assert_eq!(finished["content"]["pipelineName"], "ci");
        assert_eq!(finished["content"]["outcome"], "success");

        // A run cancelled before any task of it began: its tasks did not
        // run, and the run's own event is all there is.
        let yaml = "name: gate\ntasks:\n  - name: wait\n    command: [\"true\"]\n";
        let dag = crate::dag::DagGraph::from_yaml(yaml).unwrap();
        let cancelled = db::create_run(&pool, &dag, yaml).await.unwrap();
        assert!(db::cancel_run(&pool, &cancelled).await.unwrap().is_some());
        assert_eq!(crate::run_end::drain_once(&pool, &part).await.unwrap(), 1);
        let after: Vec<Value> = got_now(&got_all)
            .into_iter()
            .skip(3)
            .map(|(_, e)| e)
            .collect();
        assert_eq!(after.len(), 1, "{after:?}");
        assert_eq!(after[0]["context"]["type"], PIPELINE_RUN_FINISHED);
        assert_eq!(after[0]["subject"]["content"]["outcome"], "cancel");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A run's events have a minute, and a tenth of a second more for each,
    /// to half an hour; within the lease of this part.
    #[test]
    fn a_runs_events_have_the_time_they_take() {
        assert_eq!(budget(1), Duration::from_millis(60_100));
        assert_eq!(budget(1_201), Duration::from_millis(180_100));
        assert_eq!(budget(100_001), MAX_BUDGET);
        assert!(MAX_BUDGET.as_secs() as i64 + 60 <= LEASE_SECS);
    }

    /// A task run's event for each task that ran, in the order they
    /// finished, then the run's, each at the time it happened, with ids of
    /// the end's outbox row; and the failures named.
    #[test]
    fn a_runs_end_is_its_tasks_in_the_order_they_finished_then_the_run() {
        let tasks = [
            task("test", TaskStatus::Failed, "2026-10-19T10:00:05+00:00"),
            task("build", TaskStatus::Succeeded, "2026-10-19T10:00:03+00:00"),
            task("docs", TaskStatus::Skipped, "2026-10-19T10:00:06+00:00"),
        ];
        let events = run_events("dagron", "e-7", &run(RunStatus::Failed), &tasks);
        let summary: Vec<(String, String, String, String)> = events
            .iter()
            .map(|e| {
                (
                    e["context"]["id"].as_str().unwrap().to_string(),
                    e["context"]["type"].as_str().unwrap().to_string(),
                    e["subject"]["id"].as_str().unwrap().to_string(),
                    e["context"]["timestamp"].as_str().unwrap().to_string(),
                )
            })
            .collect();
        assert_eq!(
            summary,
            [
                (
                    "e-7/0".into(),
                    TASK_RUN_FINISHED.into(),
                    "task-build".into(),
                    "2026-10-19T10:00:03+00:00".into()
                ),
                (
                    "e-7/1".into(),
                    TASK_RUN_FINISHED.into(),
                    "task-test".into(),
                    "2026-10-19T10:00:05+00:00".into()
                ),
                (
                    "e-7/2".into(),
                    PIPELINE_RUN_FINISHED.into(),
                    "run-1".into(),
                    "2026-10-19T10:00:09.5+00:00".into()
                ),
            ],
            "the skipped task did not run"
        );
        assert_eq!(
            events[1]["subject"]["content"]["pipelineRun"]["id"],
            "run-1"
        );
        assert_eq!(events[2]["subject"]["content"]["outcome"], "failure");
        assert_eq!(events[2]["subject"]["content"]["errors"], "failed: test");
        assert_eq!(events[2]["subject"]["content"]["pipelineName"], "ci");
        assert!(events[0]["subject"]["content"].get("errors").is_none());
    }
}
