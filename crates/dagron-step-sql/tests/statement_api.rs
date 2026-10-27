//! The statement-API transports end to end: the built binary, driven by its
//! environment the way a task runs it, against a stand-in for each vendor's API.
//!
//! The stand-ins answer in each API's documented shapes: Snowflake's 202-then-200
//! with partitions, BigQuery's `jobComplete` and page tokens, Databricks' states
//! and chunks. They are not the services. These tests pin what the step does with
//! those shapes — poll, page, budget, cancel, refuse. Whether each service still
//! answers in them is what a run against the service itself shows, and nothing
//! here stands in for that.

use std::process::Output;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::IntoResponse;
use axum::Router;
use serde_json::{json, Value};

/// One request the stand-in received.
#[derive(Clone, Debug)]
struct Seen {
    method: String,
    path: String,
    query: String,
    headers: HeaderMap,
    body: String,
}

impl Seen {
    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap_or(Value::Null)
    }

    fn header(&self, name: &str) -> &str {
        self.headers.get(name).and_then(|v| v.to_str().ok()).unwrap_or("")
    }

    fn is(&self, method: &str, path: &str) -> bool {
        self.method == method && self.path == path
    }
}

type Respond = dyn Fn(&Seen) -> (u16, String) + Send + Sync;

#[derive(Clone)]
struct Mock {
    seen: Arc<Mutex<Vec<Seen>>>,
    respond: Arc<Respond>,
}

async fn answer(
    State(mock): State<Mock>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let seen = Seen {
        method: method.to_string(),
        path: uri.path().to_string(),
        query: uri.query().unwrap_or("").to_string(),
        headers,
        body: String::from_utf8_lossy(&body).into_owned(),
    };
    mock.seen.lock().unwrap().push(seen.clone());
    let (status, body) = (mock.respond)(&seen);
    let mut resp = (StatusCode::from_u16(status).unwrap(), [("content-type", "application/json")], body.clone())
        .into_response();
    // A 3xx answer's body is where it points, so a test can send a redirect.
    if (300..400).contains(&status) {
        resp.headers_mut().insert("location", body.parse().unwrap());
    }
    resp
}

/// Serve `respond` on a loopback port; the URL to reach it, and what it saw.
async fn serve(
    respond: impl Fn(&Seen) -> (u16, String) + Send + Sync + 'static,
) -> (String, Arc<Mutex<Vec<Seen>>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mock = Mock { seen: Arc::new(Mutex::new(Vec::new())), respond: Arc::new(respond) };
    let app = Router::new().fallback(answer).with_state(mock.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), mock.seen)
}

fn ok(v: Value) -> (u16, String) {
    (200, v.to_string())
}

/// Run the step with exactly this environment. Nothing is inherited, so no
/// proxy setting and no stray `SQL_*` variable reaches it.
async fn step(env: &[(&str, &str)]) -> Output {
    let env: Vec<(String, String)> =
        env.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    tokio::task::spawn_blocking(move || {
        std::process::Command::new(env!("CARGO_BIN_EXE_dagron-step-sql"))
            .env_clear()
            .envs(env)
            .output()
            .unwrap()
    })
    .await
    .unwrap()
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// Everything the step printed. The logs share stdout with a scalar's value.
fn printed(out: &Output) -> String {
    format!("{}{}", String::from_utf8_lossy(&out.stdout), stderr(out))
}

/// The last line on stdout, which is where `scalar` prints its value.
fn last_line(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).lines().last().unwrap_or("").to_string()
}

fn artifacts(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("dagron-step-sql-api-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn ndjson(path: &std::path::Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

// ---------------------------------------------------------------------------
// Snowflake
// ---------------------------------------------------------------------------

/// The whole path: submitted async, polled while it runs, then every partition
/// read in order into the artifact.
#[tokio::test(flavor = "multi_thread")]
async fn snowflake_polls_then_reads_every_partition_into_the_artifact() {
    let polls = Arc::new(AtomicUsize::new(0));
    let p = polls.clone();
    let (base, seen) = serve(move |s| {
        if s.is("POST", "/api/v2/statements") {
            return (202, json!({"code": "333334", "statementHandle": "h-1"}).to_string());
        }
        if s.is("GET", "/api/v2/statements/h-1") {
            return match s.query.as_str() {
                "partition=1" => ok(json!({"data": [["2", "b"]]})),
                "partition=2" => ok(json!({"data": [["3", null]]})),
                _ if p.fetch_add(1, Ordering::SeqCst) == 0 => {
                    (202, json!({"code": "333334", "statementHandle": "h-1"}).to_string())
                }
                _ => ok(json!({
                    "statementHandle": "h-1",
                    "resultSetMetaData": {
                        "rowType": [{"name": "N"}, {"name": "S"}],
                        "partitionInfo": [{"rowCount": 1}, {"rowCount": 1}, {"rowCount": 1}],
                    },
                    "data": [["1", "a"]],
                })),
            };
        }
        (404, "{}".into())
    })
    .await;
    let dir = artifacts("sf-rows");
    let out = step(&[
        ("SQL_ENGINE", "snowflake"),
        ("SQL_DSN", &format!("{base}/?warehouse=WH&database=DB&role=R")),
        ("SQL_PASSWORD", "pat-123\n"),
        ("SQL_MODE", "rows"),
        ("SQL_STATEMENT", "select n, s from t"),
        ("DAGRON_ARTIFACTS", dir.to_str().unwrap()),
    ])
    .await;
    assert!(out.status.success(), "{}", printed(&out));
    assert_eq!(
        ndjson(&dir.join("result.ndjson")),
        vec![json!({"N": "1", "S": "a"}), json!({"N": "2", "S": "b"}), json!({"N": "3", "S": null})],
        "every partition, in order"
    );

    let seen = seen.lock().unwrap();
    let submit = &seen[0];
    assert!(submit.is("POST", "/api/v2/statements"));
    assert_eq!(submit.query, "async=true");
    assert_eq!(submit.header("authorization"), "Bearer pat-123", "trimmed of the secret file's newline");
    assert_eq!(submit.header("x-snowflake-authorization-token-type"), "PROGRAMMATIC_ACCESS_TOKEN");
    assert!(submit.header("user-agent").starts_with("dagron-step-sql/"));
    let body = submit.json();
    assert_eq!(body["statement"], "select n, s from t");
    assert_eq!((body["warehouse"].as_str(), body["database"].as_str(), body["role"].as_str()), (Some("WH"), Some("DB"), Some("R")));
    assert_eq!(body["timeout"], 300, "Snowflake is given the deadline as its own");
    assert!(seen.iter().all(|s| !s.path.ends_with("/cancel")), "nothing to cancel: it finished");
    std::fs::remove_dir_all(&dir).ok();
}

/// `script` lifts Snowflake's one-statement default, and a finished `exec` or
/// `script` is settled by its status, so its body is never read. Here the body is
/// not even JSON.
#[tokio::test(flavor = "multi_thread")]
async fn a_snowflake_script_runs_as_one_request_and_its_result_is_not_read() {
    let (base, seen) = serve(|s| match s.is("POST", "/api/v2/statements") {
        true => (200, "a result body that exec has no business parsing".into()),
        false => (404, "{}".into()),
    })
    .await;
    let out = step(&[
        ("SQL_ENGINE", "snowflake"),
        ("SQL_DSN", &base),
        ("SQL_PASSWORD", "tok"),
        ("SQL_TOKEN_TYPE", "oauth"),
        ("SQL_MODE", "script"),
        ("SQL_STATEMENT", "BEGIN TRANSACTION;\nDELETE FROM t WHERE d = 1;\nINSERT INTO t SELECT 1;\nCOMMIT;"),
    ])
    .await;
    assert!(out.status.success(), "{}", printed(&out));
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1, "one request for the whole script: {seen:?}");
    assert_eq!(seen[0].json()["parameters"]["MULTI_STATEMENT_COUNT"], "0");
    assert_eq!(seen[0].header("x-snowflake-authorization-token-type"), "OAUTH");
}

/// A failed statement's own message is the error, with what an operator needs to
/// find it in Snowflake's query history. It already ended, so nothing is cancelled.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_snowflake_statement_reports_its_message_and_handle() {
    let (base, seen) = serve(|s| {
        if s.is("POST", "/api/v2/statements") {
            return (202, json!({"statementHandle": "h-2"}).to_string());
        }
        (
            422,
            json!({
                "code": "002003",
                "sqlState": "42S02",
                "message": "SQL compilation error: Object 'MISSING' does not exist or not authorized.",
                "statementHandle": "h-2",
            })
            .to_string(),
        )
    })
    .await;
    let out = step(&[
        ("SQL_ENGINE", "snowflake"),
        ("SQL_DSN", &base),
        ("SQL_PASSWORD", "tok"),
        ("SQL_STATEMENT", "insert into missing select 1"),
    ])
    .await;
    assert!(!out.status.success());
    let e = stderr(&out);
    assert!(e.contains("Object 'MISSING' does not exist"), "{e}");
    assert!(e.contains("SQLSTATE 42S02") && e.contains("statement h-2"), "{e}");
    assert!(seen.lock().unwrap().iter().all(|s| !s.path.ends_with("/cancel")));
}

/// The budget refuses the row that breaks it, across a partition boundary, and
/// the artifact path is left empty: the same promise as the other transports.
#[tokio::test(flavor = "multi_thread")]
async fn a_result_over_the_row_budget_leaves_nothing_at_the_artifact_path() {
    let (base, seen) = serve(|s| {
        if s.is("POST", "/api/v2/statements") {
            return ok(json!({
                "statementHandle": "h-3",
                "resultSetMetaData": {
                    "rowType": [{"name": "N"}],
                    "partitionInfo": [{"rowCount": 2}, {"rowCount": 2}, {"rowCount": 2}],
                },
                "data": [["1"], ["2"]],
            }));
        }
        match s.query.as_str() {
            "partition=1" => ok(json!({"data": [["3"], ["4"]]})),
            _ => ok(json!({"data": [["5"], ["6"]]})),
        }
    })
    .await;
    let dir = artifacts("sf-budget");
    let out = step(&[
        ("SQL_ENGINE", "snowflake"),
        ("SQL_DSN", &base),
        ("SQL_PASSWORD", "tok"),
        ("SQL_MODE", "rows"),
        ("SQL_MAX_ROWS", "3"),
        ("SQL_STATEMENT", "select n from t"),
        ("DAGRON_ARTIFACTS", dir.to_str().unwrap()),
    ])
    .await;
    assert!(!out.status.success());
    assert!(stderr(&out).contains("SQL_MAX_ROWS (3)"), "{}", stderr(&out));
    assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0, "no artifact and no partial");
    let partitions: Vec<String> = seen.lock().unwrap().iter().map(|s| s.query.clone()).filter(|q| q.starts_with("partition")).collect();
    assert_eq!(partitions, ["partition=1"], "the partition after the refusal was never fetched");
    std::fs::remove_dir_all(&dir).ok();
}

// ---------------------------------------------------------------------------
// BigQuery
// ---------------------------------------------------------------------------

/// A scalar gate over a job that had to be polled, polled where the job runs.
#[tokio::test(flavor = "multi_thread")]
async fn a_bigquery_scalar_after_a_poll_prints_one_value() {
    let (base, seen) = serve(|s| {
        if s.is("POST", "/bigquery/v2/projects/p1/queries") {
            return ok(json!({
                "jobComplete": false,
                "jobReference": {"projectId": "p1", "jobId": "job_1", "location": "EU"},
            }));
        }
        if s.is("GET", "/bigquery/v2/projects/p1/queries/job_1") {
            return ok(json!({
                "jobComplete": true,
                "jobReference": {"projectId": "p1", "jobId": "job_1", "location": "EU"},
                "schema": {"fields": [{"name": "ok", "type": "BOOLEAN"}]},
                "rows": [{"f": [{"v": "true"}]}],
            }));
        }
        (404, "{}".into())
    })
    .await;
    let out = step(&[
        ("SQL_ENGINE", "bigquery"),
        ("SQL_DSN", &format!("{base}/?project=p1&dataset=marts")),
        ("SQL_PASSWORD", "ya29.stored"),
        ("SQL_MODE", "scalar"),
        ("SQL_STATEMENT", "select count(*) > 0 from events where dt = '2026-06-20'"),
    ])
    .await;
    assert!(out.status.success(), "{}", printed(&out));
    assert_eq!(last_line(&out), "true");

    let seen = seen.lock().unwrap();
    let body = seen[0].json();
    assert_eq!(body["maxResults"], 2, "the answer and the row that would disprove it");
    assert_eq!(body["defaultDataset"]["datasetId"], "marts");
    assert_eq!(seen[0].header("authorization"), "Bearer ya29.stored");
    assert!(seen[1].query.contains("location=EU"), "the job's location: {}", seen[1].query);
}

/// "Exactly one row" holds across a page boundary: the second row is on the next
/// page, and the gate still refuses.
#[tokio::test(flavor = "multi_thread")]
async fn a_bigquery_scalar_refuses_a_second_row_on_the_next_page() {
    let (base, _) = serve(|s| {
        let fields = json!({"fields": [{"name": "n", "type": "INTEGER"}]});
        if s.method == "POST" {
            return ok(json!({
                "jobComplete": true,
                "jobReference": {"projectId": "p1", "jobId": "job_2"},
                "schema": fields,
                "rows": [{"f": [{"v": "1"}]}],
                "pageToken": "page-2",
            }));
        }
        ok(json!({"jobComplete": true, "schema": fields, "rows": [{"f": [{"v": "2"}]}]}))
    })
    .await;
    let out = step(&[
        ("SQL_ENGINE", "bigquery"),
        ("SQL_DSN", &format!("{base}/?project=p1")),
        ("SQL_PASSWORD", "tok"),
        ("SQL_MODE", "scalar"),
        ("SQL_STATEMENT", "select n from t"),
    ])
    .await;
    assert!(!out.status.success());
    assert!(stderr(&out).contains("exactly one row"), "{}", stderr(&out));
    assert!(!String::from_utf8_lossy(&out.stdout).lines().any(|l| l == "1"), "row one was not printed as the answer");
}

/// With no `SQL_PASSWORD`, BigQuery's token comes from the metadata server, the
/// one way to reach it with no stored secret.
#[tokio::test(flavor = "multi_thread")]
async fn bigquery_takes_its_token_from_the_metadata_server_when_none_is_stored() {
    let (base, seen) = serve(|s| {
        if s.path == "/computeMetadata/v1/instance/service-accounts/default/token" {
            if s.header("metadata-flavor") != "Google" {
                return (403, "{}".into());
            }
            return ok(json!({"access_token": "ya29.from-metadata", "expires_in": 3599, "token_type": "Bearer"}));
        }
        ok(json!({"jobComplete": true, "jobReference": {"projectId": "p1", "jobId": "job_3"}}))
    })
    .await;
    let out = step(&[
        ("SQL_ENGINE", "bigquery"),
        ("SQL_DSN", &format!("{base}/?project=p1")),
        ("GCE_METADATA_HOST", base.trim_start_matches("http://")),
        ("SQL_STATEMENT", "delete from t where dt = '2026-06-20'"),
    ])
    .await;
    assert!(out.status.success(), "{}", printed(&out));
    let seen = seen.lock().unwrap();
    let query = seen.iter().find(|s| s.path.ends_with("/queries")).expect("the statement was sent");
    assert_eq!(query.header("authorization"), "Bearer ya29.from-metadata");
    assert_eq!(query.json()["maxResults"], 0, "exec asks for no rows");
}

// ---------------------------------------------------------------------------
// Databricks
// ---------------------------------------------------------------------------

/// Chunks are read in order, and the store is asked for one row past the budget.
#[tokio::test(flavor = "multi_thread")]
async fn databricks_reads_every_chunk_and_asks_for_one_row_past_the_budget() {
    let (base, seen) = serve(|s| {
        if s.is("POST", "/api/2.0/sql/statements/") {
            return ok(json!({"statement_id": "s-1", "status": {"state": "PENDING"}}));
        }
        if s.is("GET", "/api/2.0/sql/statements/s-1") {
            return ok(json!({
                "statement_id": "s-1",
                "status": {"state": "SUCCEEDED"},
                "manifest": {"schema": {"columns": [{"name": "dt", "position": 0}, {"name": "n", "position": 1}]}},
                "result": {"chunk_index": 0, "data_array": [["2026-06-20", "1"]], "next_chunk_index": 1},
            }));
        }
        if s.is("GET", "/api/2.0/sql/statements/s-1/result/chunks/1") {
            return ok(json!({"chunk_index": 1, "data_array": [["2026-06-21", "2"]]}));
        }
        (404, "{}".into())
    })
    .await;
    let dir = artifacts("dbx-rows");
    let out = step(&[
        ("SQL_ENGINE", "databricks"),
        ("SQL_DSN", &format!("{base}/?warehouse_id=w1&catalog=main&schema=marts")),
        ("SQL_PASSWORD", "dapi-123"),
        ("SQL_MODE", "rows"),
        ("SQL_MAX_ROWS", "50"),
        ("SQL_OUTPUT_NAME", "daily.ndjson"),
        ("SQL_STATEMENT", "select dt, n from daily"),
        ("DAGRON_ARTIFACTS", dir.to_str().unwrap()),
    ])
    .await;
    assert!(out.status.success(), "{}", printed(&out));
    assert_eq!(
        ndjson(&dir.join("daily.ndjson")),
        vec![json!({"dt": "2026-06-20", "n": "1"}), json!({"dt": "2026-06-21", "n": "2"})]
    );
    let seen = seen.lock().unwrap();
    let body = seen[0].json();
    assert_eq!(body["row_limit"], 51);
    assert_eq!(body["warehouse_id"], "w1");
    assert_eq!(seen[0].header("authorization"), "Bearer dapi-123");
    std::fs::remove_dir_all(&dir).ok();
}

/// A script is one compound statement, so the API's one-statement rule holds.
#[tokio::test(flavor = "multi_thread")]
async fn a_databricks_script_is_sent_as_one_compound_statement() {
    let (base, seen) = serve(|_| ok(json!({"statement_id": "s-2", "status": {"state": "SUCCEEDED"}}))).await;
    let out = step(&[
        ("SQL_ENGINE", "databricks"),
        ("SQL_DSN", &format!("{base}/?warehouse_id=w1")),
        ("SQL_PASSWORD", "dapi-123"),
        ("SQL_MODE", "script"),
        ("SQL_STATEMENT", "DELETE FROM t WHERE dt IN ('2026-06-20');\nINSERT INTO t (dt, n) SELECT dt, n FROM m;"),
    ])
    .await;
    assert!(out.status.success(), "{}", printed(&out));
    assert_eq!(
        seen.lock().unwrap()[0].json()["statement"],
        "BEGIN\nDELETE FROM t WHERE dt IN ('2026-06-20');\nINSERT INTO t (dt, n) SELECT dt, n FROM m;\nEND"
    );
}

/// The handle is what lets the step stop a statement that outlives it.
#[tokio::test(flavor = "multi_thread")]
async fn a_statement_still_running_at_the_deadline_is_cancelled() {
    let (base, seen) = serve(|s| match s.path.ends_with("/cancel") {
        true => ok(json!({})),
        false => ok(json!({"statement_id": "s-9", "status": {"state": "RUNNING"}})),
    })
    .await;
    let out = step(&[
        ("SQL_ENGINE", "databricks"),
        ("SQL_DSN", &format!("{base}/?warehouse_id=w1")),
        ("SQL_PASSWORD", "dapi-123"),
        ("SQL_TIMEOUT_SECS", "1"),
        ("SQL_STATEMENT", "optimize events"),
    ])
    .await;
    assert!(!out.status.success());
    let e = stderr(&out);
    assert!(e.contains("SQL_TIMEOUT_SECS (1s)") && e.contains("cancelled") && e.contains("s-9"), "{e}");
    assert!(
        seen.lock().unwrap().iter().any(|s| s.is("POST", "/api/2.0/sql/statements/s-9/cancel")),
        "the cancel was sent"
    );
}

/// A poll is a read, so a 503 on the way is retried rather than ending the step
/// while the statement it was asking after carries on.
#[tokio::test(flavor = "multi_thread")]
async fn a_poll_that_fails_in_transit_is_retried() {
    let polls = Arc::new(AtomicUsize::new(0));
    let p = polls.clone();
    let (base, _) = serve(move |s| {
        if s.method == "POST" {
            return ok(json!({"statement_id": "s-3", "status": {"state": "PENDING"}}));
        }
        match p.fetch_add(1, Ordering::SeqCst) {
            0 => (503, json!({"error_code": "TEMPORARILY_UNAVAILABLE", "message": "try again"}).to_string()),
            _ => ok(json!({"statement_id": "s-3", "status": {"state": "SUCCEEDED"}})),
        }
    })
    .await;
    let out = step(&[
        ("SQL_ENGINE", "databricks"),
        ("SQL_DSN", &format!("{base}/?warehouse_id=w1")),
        ("SQL_PASSWORD", "dapi-123"),
        ("SQL_STATEMENT", "insert into t select 1"),
    ])
    .await;
    assert!(out.status.success(), "{}", printed(&out));
    assert_eq!(polls.load(Ordering::SeqCst), 2, "one 503, then the answer");
}

/// A poll still answered "try again" after its retries says nothing about the
/// statement, which may still be running, so it is cancelled on the way out.
#[tokio::test(flavor = "multi_thread")]
async fn a_poll_that_stays_unavailable_cancels_the_statement() {
    let (base, seen) = serve(|s| match (s.method.as_str(), s.path.ends_with("/cancel")) {
        ("POST", true) => ok(json!({})),
        ("POST", false) => ok(json!({"statement_id": "s-4", "status": {"state": "PENDING"}})),
        _ => (503, json!({"error_code": "TEMPORARILY_UNAVAILABLE", "message": "try again"}).to_string()),
    })
    .await;
    let out = step(&[
        ("SQL_ENGINE", "databricks"),
        ("SQL_DSN", &format!("{base}/?warehouse_id=w1")),
        ("SQL_PASSWORD", "dapi-123"),
        ("SQL_STATEMENT", "insert into t select 1"),
    ])
    .await;
    assert!(!out.status.success());
    let e = stderr(&out);
    assert!(e.contains("s-4") && e.contains("cancelled") && e.contains("503"), "{e}");
    let seen = seen.lock().unwrap();
    assert_eq!(seen.iter().filter(|s| s.method == "GET").count(), 3, "the poll was retried first");
    assert!(seen.iter().any(|s| s.is("POST", "/api/2.0/sql/statements/s-4/cancel")), "the cancel was sent");
}

/// A redirect is a URL the store handed back, and the step goes nowhere the DSN
/// did not name. It is an error naming the status, and the target is never asked.
#[tokio::test(flavor = "multi_thread")]
async fn a_redirect_is_refused_not_followed() {
    let (base, seen) = serve(|s| match s.path.as_str() {
        "/elsewhere" => ok(json!({"statement_id": "s-x", "status": {"state": "SUCCEEDED"}})),
        _ => (307, "/elsewhere".into()),
    })
    .await;
    let out = step(&[
        ("SQL_ENGINE", "databricks"),
        ("SQL_DSN", &format!("{base}/?warehouse_id=w1")),
        ("SQL_PASSWORD", "dapi-123"),
        ("SQL_STATEMENT", "insert into t select 1"),
    ])
    .await;
    assert!(!out.status.success());
    assert!(stderr(&out).contains("HTTP 307"), "{}", stderr(&out));
    assert!(seen.lock().unwrap().iter().all(|s| s.path != "/elsewhere"), "the redirect was not followed");
}

/// A missing token is refused before anything is sent.
#[tokio::test(flavor = "multi_thread")]
async fn no_token_is_refused_before_any_request() {
    let (base, seen) = serve(|_| ok(json!({}))).await;
    let out = step(&[
        ("SQL_ENGINE", "databricks"),
        ("SQL_DSN", &format!("{base}/?warehouse_id=w1")),
        ("SQL_STATEMENT", "select 1"),
    ])
    .await;
    assert!(!out.status.success());
    assert!(stderr(&out).contains("SQL_PASSWORD is not set"), "{}", stderr(&out));
    assert!(seen.lock().unwrap().is_empty());
}
