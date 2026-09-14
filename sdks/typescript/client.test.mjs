import { test, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { Client, DagronError } from "./index.mjs";

// ── fake gateway ────────────────────────────────────────────────────────────
//
// Records each request and replies from a `responses` table keyed by
// "METHOD PATH". An unconfigured route returns 500 so a typo / client-side path
// regression fails loudly instead of passing as a false green (mirrors the
// Python SDK's fake gateway).

let server;
let base;
let requests;
let responses;
let sseBody;
let sequences;

function respond(method, path, status, payload) {
  responses.set(`${method} ${path}`, { status, payload });
}

/** Queue one response per call to this route, in order (for pagination). */
function respondEach(method, path, ...queued) {
  sequences.set(`${method} ${path}`, queued.slice());
}

function last() {
  return requests[requests.length - 1];
}

beforeEach(async () => {
  requests = [];
  responses = new Map();
  sequences = new Map();
  sseBody = "";
  server = http.createServer((req, res) => {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      const url = new URL(req.url, "http://x");
      const query = {};
      for (const [k, v] of url.searchParams.entries()) (query[k] ??= []).push(v);
      requests.push({
        method: req.method,
        path: url.pathname,
        query,
        headers: req.headers,
        body: Buffer.concat(chunks).toString("utf8"),
        // The utf8 view mangles non-text bytes, so keep the raw buffer for the
        // assertions that care (artifact uploads).
        rawBody: Buffer.concat(chunks),
      });

      if (url.pathname.endsWith("/stream")) {
        res.writeHead(200, { "content-type": "text/event-stream" });
        res.end(sseBody);
        return;
      }

      // A queued sequence answers one call each, so a paginating client can be
      // driven through more than one page of the same route.
      const key = `${req.method} ${url.pathname}`;
      const queued = sequences.get(key);
      const hit = queued?.length ? queued.shift() : responses.get(key);
      if (!hit) {
        res.writeHead(500, { "content-type": "text/plain" });
        res.end(`unconfigured fake-gateway route: ${req.method} ${url.pathname}`);
        return;
      }
      const { status, payload } = hit;
      if (payload === null || payload === undefined) {
        res.writeHead(status);
        res.end();
        return;
      }
      if (payload instanceof Uint8Array) {
        res.writeHead(status, { "content-type": "application/octet-stream" });
        res.end(Buffer.from(payload));
      } else if (typeof payload === "object") {
        const b = JSON.stringify(payload);
        res.writeHead(status, { "content-type": "application/json" });
        res.end(b);
      } else {
        res.writeHead(status, { "content-type": "text/plain" });
        res.end(String(payload));
      }
    });
  });
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  const { address, port } = server.address();
  base = `http://${address}:${port}`;
});

afterEach(async () => {
  await new Promise((r) => server.close(r));
});

function client() {
  return new Client(base, { token: "tok" });
}

// ── construction / transport ────────────────────────────────────────────────

test("rejects a non-http(s) base_url", () => {
  assert.throws(() => new Client("file:///etc/passwd"), /http or https/);
  assert.throws(() => new Client("not a url"), /valid http/);
});

test("sends the bearer token on authed calls", async () => {
  respond("GET", "/api/me", 200, { sub: "u1" });
  await client().me();
  assert.equal(last().headers.authorization, "Bearer tok");
});

test("list query omits unset params, encodes set ones", async () => {
  respond("GET", "/api/runs", 200, []);
  await client().listRuns();
  assert.deepEqual(last().query, {});

  respond("GET", "/api/runs", 200, [{ id: "r-1" }]);
  await client().listRuns({ status: "failed", limit: 10, offset: 20 });
  assert.deepEqual(last().query, { status: ["failed"], limit: ["10"], offset: ["20"] });
});

test("run logs encode the filter and task scope", async () => {
  respond("GET", "/api/runs/r-1/logs", 200, { lines: [] });
  await client().getRunLogs("r-1", {
    level: ["error", "warn"],
    context: 2,
    tail: true,
    case: false,
    regex: "rows=\\d+",
    tasks: ["extract", "load"],
  });
  const req = last();
  assert.equal(req.path, "/api/runs/r-1/logs");
  assert.deepEqual(req.query.level, ["error,warn"]);
  assert.deepEqual(req.query.context, ["2"]);
  assert.deepEqual(req.query.tail, ["1"]);
  assert.deepEqual(req.query.regex, ["rows=\\d+"]);
  assert.deepEqual(req.query.task, ["extract,load"]);
  // `case: false` is dropped, not sent as 0 — an explicit `case=0` would still
  // count as "the caller filtered" and change server behaviour.
  assert.equal(req.query.case, undefined);
});

test("an unfiltered log read sends no query at all", async () => {
  respond("GET", "/api/runs/r-1/logs", 200, { lines: [] });
  await client().getRunLogs("r-1");
  assert.deepEqual(last().query, {});
});

test("task logs combine offset with the filter", async () => {
  respond("GET", "/api/runs/r-1/tasks/t-1/logs", 200, { output: "" });
  await client().getTaskLogs("r-1", "t-1", { offset: 120, level: "error" });
  assert.deepEqual(last().query.offset, ["120"]);
  assert.deepEqual(last().query.level, ["error"]);
});

test("an unknown log filter parameter throws", async () => {
  // Silently dropping a typo'd filter would return an unfiltered response the
  // caller reads as filtered.
  await assert.rejects(() => client().getRunLogs("r-1", { levl: "error" }), TypeError);
});

test("path segments are percent-encoded (no traversal / wrong endpoint)", async () => {
  respond("GET", "/api/runs/a%2Fb", 200, { id: "a/b" });
  const run = await client().getRun("a/b");
  assert.equal(run.id, "a/b");
  assert.equal(last().path, "/api/runs/a%2Fb");
});

test("submit_run posts the spec under the yaml key", async () => {
  respond("POST", "/api/runs", 200, { run_id: "r-9" });
  const id = await client().submitRun("name: y\ntasks: []\n");
  assert.equal(id, "r-9");
  assert.deepEqual(JSON.parse(last().body), { yaml: "name: y\ntasks: []\n" });
});

test("submit_run sends parameters only when given", async () => {
  respond("POST", "/api/runs", 200, { run_id: "r-10" });
  await client().submitRun("name: y\n", { parameters: { date: "2026-08-18" } });
  assert.deepEqual(JSON.parse(last().body).parameters, { date: "2026-08-18" });

  // Omitted (and empty) parameters must not appear at all: the gateway's
  // pre-existing body shape is { yaml } and every older server has to keep
  // accepting exactly that.
  respond("POST", "/api/runs", 200, { run_id: "r-11" });
  await client().submitRun("name: y\n", { parameters: {} });
  assert.deepEqual(Object.keys(JSON.parse(last().body)), ["yaml"]);
});

test("submit_run sends the idempotency key as a header, and none without one", async () => {
  respond("POST", "/api/runs", 200, { run_id: "r-12" });
  await client().submitRun("name: y\n", { idempotencyKey: "job-42" });
  assert.equal(last().headers["idempotency-key"], "job-42");

  // No key, no header — the endpoint stays non-idempotent by default, and an
  // empty key is a 400 server-side, so one must never be invented here.
  respond("POST", "/api/runs", 200, { run_id: "r-13" });
  await client().submitRun("name: y\n");
  assert.equal(last().headers["idempotency-key"], undefined);
});

test("an explicitly empty idempotency key is forwarded, not dropped", async () => {
  // `""` is a 400 server-side. A truthiness check would drop it and submit
  // without idempotency — a non-idempotent request the caller believes is
  // retry-safe. An explicit empty string must reach the server as the header it
  // is, so the 400 surfaces; only an absent key means "no header".
  respond("POST", "/api/runs", 400, { error: "Idempotency-Key must not be empty" });
  await assert.rejects(client().submitRun("name: y\n", { idempotencyKey: "" }));
  assert.equal(last().headers["idempotency-key"], "");
});

test("a caller header cannot displace authorization", async () => {
  respond("POST", "/api/runs", 200, { run_id: "r-14" });
  await client()._request("POST", "/api/runs", {
    body: { yaml: "x" },
    headers: { Authorization: "Bearer stolen" },
  });
  assert.equal(last().headers.authorization, "Bearer tok");
});

// ── 0.3.0 surface: approvals ────────────────────────────────────────────────

test("approve/reject task hit the right routes and return the resolution", async () => {
  respond("POST", "/api/runs/r-1/tasks/t-1/approve", 200, {
    run_id: "r-1",
    task_id: "t-1",
    resolution: "approved",
  });
  assert.equal((await client().approveTask("r-1", "t-1")).resolution, "approved");

  respond("POST", "/api/runs/r-1/tasks/t-2/reject", 200, {
    run_id: "r-1",
    task_id: "t-2",
    resolution: "rejected",
  });
  assert.equal((await client().rejectTask("r-1", "t-2")).resolution, "rejected");
});

// ── 0.3.0 surface: backfill jobs ────────────────────────────────────────────

test("create_backfill posts schedule_id/from/to/max_runs", async () => {
  respond("POST", "/api/backfills", 201, { id: "bf-1", status: "running" });
  const job = await client().createBackfill(
    "s-1",
    "2026-01-01T00:00:00Z",
    "2026-01-02T00:00:00Z",
    { maxRuns: 100 },
  );
  assert.equal(job.id, "bf-1");
  assert.deepEqual(JSON.parse(last().body), {
    schedule_id: "s-1",
    from: "2026-01-01T00:00:00Z",
    to: "2026-01-02T00:00:00Z",
    max_runs: 100,
  });
});

test("create_backfill omits unset max_runs", async () => {
  respond("POST", "/api/backfills", 201, { id: "bf-2" });
  await client().createBackfill("s-1", "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z");
  assert.ok(!("max_runs" in JSON.parse(last().body)));
});

test("list/get/cancel backfill", async () => {
  respond("GET", "/api/backfills", 200, [{ id: "bf-1" }]);
  const rows = await client().listBackfills({ scheduleId: "s-1" });
  assert.equal(rows[0].id, "bf-1");
  assert.deepEqual(last().query, { schedule_id: ["s-1"] });

  respond("GET", "/api/backfills/bf-1", 200, { id: "bf-1", fired: 3 });
  assert.equal((await client().getBackfill("bf-1")).fired, 3);

  respond("POST", "/api/backfills/bf-1/cancel", 200, { id: "bf-1", status: "cancelled" });
  assert.equal((await client().cancelBackfill("bf-1")).status, "cancelled");
});

// ── 0.3.0 surface: git-repo path ────────────────────────────────────────────

test("connect_git_repo passes path when given, omits it otherwise", async () => {
  respond("POST", "/api/git-repos", 201, { id: "g-1" });
  await client().connectGitRepo("https://github.com/o/r", {
    branch: "main",
    autoSync: true,
    path: "pipelines",
  });
  assert.deepEqual(JSON.parse(last().body), {
    url: "https://github.com/o/r",
    branch: "main",
    auto_sync: true,
    path: "pipelines",
  });

  respond("POST", "/api/git-repos", 201, { id: "g-2" });
  await client().connectGitRepo("https://github.com/o/r");
  assert.ok(!("path" in JSON.parse(last().body)));
});

// ── errors + SSE ────────────────────────────────────────────────────────────

test("error body {error: ...} is unwrapped into DagronError", async () => {
  respond("POST", "/api/runs/r-1/cancel", 409, { error: "already terminal" });
  await assert.rejects(client().cancelRun("r-1"), (err) => {
    assert.ok(err instanceof DagronError);
    assert.equal(err.status, 409);
    assert.equal(err.message, "dagron-api 409: already terminal");
    return true;
  });
});

test("stream_run parses SSE events (event + JSON/raw data)", async () => {
  sseBody =
    "event: task\n" +
    'data: {"task": "a", "status": "running"}\n' +
    "\n" +
    ": keep-alive\n" +
    "event: resync\n" +
    "data: lagged\n" +
    "\n";
  const events = [];
  for await (const ev of client().streamRun("r-1")) events.push(ev);
  assert.deepEqual(events[0], { event: "task", data: { task: "a", status: "running" } });
  assert.deepEqual(events[1], { event: "resync", data: "lagged" });
  const req = last();
  assert.equal(req.method, "GET");
  assert.equal(req.path, "/api/runs/r-1/stream");
  assert.equal(req.headers.authorization, "Bearer tok");
});

// ── construction from the environment ───────────────────────────────────────

test("fromEnv reads the url and token", () => {
  const saved = { url: process.env.DAGRON_API_URL, token: process.env.DAGRON_TOKEN };
  try {
    process.env.DAGRON_API_URL = "http://gw:8080/";
    process.env.DAGRON_TOKEN = "dgp_abc";
    const api = Client.fromEnv();
    assert.equal(api.baseUrl, "http://gw:8080");
    assert.equal(api.token, "dgp_abc");

    delete process.env.DAGRON_TOKEN;
    assert.equal(Client.fromEnv().token, null);

    delete process.env.DAGRON_API_URL;
    assert.throws(() => Client.fromEnv(), /DAGRON_API_URL is not set/);
  } finally {
    if (saved.url === undefined) delete process.env.DAGRON_API_URL;
    else process.env.DAGRON_API_URL = saved.url;
    if (saved.token === undefined) delete process.env.DAGRON_TOKEN;
    else process.env.DAGRON_TOKEN = saved.token;
  }
});

test("close drops the token", () => {
  const api = new Client("http://gw:8080", { token: "tok" });
  api.close();
  assert.equal(api.token, null);
});

// ── access tokens & users ───────────────────────────────────────────────────

test("mints, lists and revokes access tokens", async () => {
  const api = new Client(base, { token: "tok" });
  respond("POST", "/api/tokens", 201, { id: "t1", token: "dgp_secret" });
  assert.equal((await api.createToken("ci", { expiresInDays: 30 })).token, "dgp_secret");
  assert.deepEqual(JSON.parse(last().body), { name: "ci", expires_in_days: 30 });

  await api.createToken("ci");
  assert.deepEqual(JSON.parse(last().body), { name: "ci" });

  respond("GET", "/api/tokens", 200, [{ id: "t1", prefix: "dgp_abc" }]);
  assert.equal((await api.listTokens())[0].prefix, "dgp_abc");

  respond("DELETE", "/api/tokens/t1", 204, null);
  assert.equal(await api.revokeToken("t1"), undefined);
});

test("lists users", async () => {
  const api = new Client(base, { token: "tok" });
  respond("GET", "/api/users", 200, [{ id: "u1", email: "a@b.c" }]);
  assert.equal((await api.listUsers())[0].email, "a@b.c");
});

// ── runs ────────────────────────────────────────────────────────────────────

test("listRuns sends the name and trigger filters", async () => {
  const api = new Client(base, { token: "tok" });
  respond("GET", "/api/runs", 200, []);
  await api.listRuns({ status: "failed", name: "etl", trigger: "schedule" });
  assert.deepEqual(last().query, { status: ["failed"], name: ["etl"], trigger: ["schedule"] });
});

test("iterRuns walks pages until a short one", async () => {
  const api = new Client(base, { token: "tok" });
  respondEach(
    "GET",
    "/api/runs",
    { status: 200, payload: [{ id: "r1" }, { id: "r2" }] },
    { status: 200, payload: [{ id: "r3" }] },
  );
  const ids = [];
  for await (const run of api.iterRuns({ pageSize: 2, status: "succeeded" })) ids.push(run.id);
  assert.deepEqual(ids, ["r1", "r2", "r3"]);
  assert.deepEqual(requests.map((r) => r.query.offset), [["0"], ["2"]]);
  // The filter rides along on every page, not just the first.
  assert.deepEqual(requests[1].query.status, ["succeeded"]);
});

test("getRunSpec returns the authored spec", async () => {
  const api = new Client(base, { token: "tok" });
  respond("GET", "/api/runs/r1/spec", 200, { yaml: "name: etl", name: "etl" });
  assert.equal((await api.getRunSpec("r1")).name, "etl");
});

test("waitRun long-polls with a server budget", async () => {
  const api = new Client(base, { token: "tok" });
  respond("GET", "/api/runs/r1/wait", 200, {
    run_id: "r1",
    status: "succeeded",
    finished: true,
    result: "42",
  });
  assert.equal((await api.waitRun("r1", { timeoutSecs: 120 })).result, "42");
  assert.deepEqual(last().query, { timeout_secs: ["120"] });

  respond("GET", "/api/runs/r1/wait", 200, { finished: false });
  await api.waitRun("r1");
  assert.deepEqual(last().query, {});
});

test("clearTask resets the subtree", async () => {
  const api = new Client(base, { token: "tok" });
  respond("POST", "/api/runs/r1/tasks/t1/clear", 200, { run_id: "r1", cleared: 3 });
  assert.equal((await api.clearTask("r1", "t1")).cleared, 3);
});

test("triage is set and cleared", async () => {
  const api = new Client(base, { token: "tok" });
  respond("POST", "/api/runs/r1/triage", 200, { triage_state: "acknowledged" });
  await api.setTriage("r1", "acknowledged", { note: "on it" });
  assert.deepEqual(JSON.parse(last().body), { state: "acknowledged", note: "on it" });

  await api.setTriage("r1", "resolved");
  assert.deepEqual(JSON.parse(last().body), { state: "resolved" });

  respond("DELETE", "/api/runs/r1/triage", 200, { triage_state: null });
  assert.equal((await api.clearTriage("r1")).triage_state, null);
});

test("archive reads and the on-demand write", async () => {
  const api = new Client(base, { token: "tok" });
  respond("GET", "/api/archive/runs", 200, [{ run_id: "r1" }]);
  await api.listArchivedRuns({ name: "etl", limit: 10 });
  assert.deepEqual(last().query, { name: ["etl"], limit: ["10"] });

  respond("GET", "/api/archive/runs/r1", 200, { run_id: "r1", index: {} });
  assert.equal((await api.getArchivedRun("r1")).run_id, "r1");

  respond("POST", "/api/runs/r1/archive", 200, { archived: true });
  assert.equal((await api.archiveRun("r1")).archived, true);
});

test("streamEvents reads the account-wide feed", async () => {
  const api = new Client(base, { token: "tok" });
  sseBody = 'event: task\ndata: {"run_id": "r1"}\n\n';
  const events = [];
  for await (const ev of api.streamEvents()) events.push(ev);
  assert.deepEqual(events, [{ event: "task", data: { run_id: "r1" } }]);
  assert.equal(last().path, "/api/events/stream");
});

// ── workflows ───────────────────────────────────────────────────────────────

test("listWorkflows filters by tag", async () => {
  const api = new Client(base, { token: "tok" });
  respond("GET", "/api/workflows", 200, []);
  await api.listWorkflows({ tag: "nightly" });
  assert.deepEqual(last().query, { tag: ["nightly"] });
});

test("runWorkflow passes parameters, and sends no body without them", async () => {
  const api = new Client(base, { token: "tok" });
  respond("POST", "/api/workflows/w1/run", 200, { run_id: "r1" });
  await api.runWorkflow("w1", { parameters: { day: "2026-01-01" } });
  assert.deepEqual(JSON.parse(last().body), { parameters: { day: "2026-01-01" } });

  await api.runWorkflow("w1");
  assert.equal(last().body, "");
});

test("workflow versions, state and runs", async () => {
  const api = new Client(base, { token: "tok" });
  respond("GET", "/api/workflows/w1/versions", 200, [{ version: 2 }]);
  assert.equal((await api.listWorkflowVersions("w1"))[0].version, 2);

  respond("POST", "/api/workflows/w1/state", 200, { id: "w1", state: "paused" });
  assert.equal((await api.setWorkflowState("w1", "paused")).state, "paused");
  assert.deepEqual(JSON.parse(last().body), { state: "paused" });

  respond("GET", "/api/workflows/w1/runs", 200, [{ id: "r1" }]);
  await api.listWorkflowRuns("w1", { limit: 5 });
  assert.deepEqual(last().query, { limit: ["5"] });
});

test("applyBundle base64-encodes every binary field", async () => {
  const api = new Client(base, { token: "tok" });
  respond("POST", "/api/workflows/bundle", 200, { applied: [] });
  await api.applyBundle(new TextEncoder().encode("manifest"), "sig", {
    "dags/etl.yaml": "name: etl",
  });
  const body = JSON.parse(last().body);
  assert.equal(Buffer.from(body.manifest_b64, "base64").toString(), "manifest");
  assert.equal(Buffer.from(body.signature_b64, "base64").toString(), "sig");
  assert.equal(body.files[0].path, "dags/etl.yaml");
  assert.equal(Buffer.from(body.files[0].content_b64, "base64").toString(), "name: etl");
});

test("workflowBadge returns SVG and sends no credential", async () => {
  const api = new Client(base, { token: "tok" });
  respond("GET", "/api/badges/etl", 200, "<svg/>");
  assert.equal(await api.workflowBadge("etl"), "<svg/>");
  assert.equal(last().headers.authorization, undefined);
});

// ── schedules ───────────────────────────────────────────────────────────────

test("createSchedule carries the whole policy", async () => {
  const api = new Client(base, { token: "tok" });
  respond("POST", "/api/schedules", 200, { id: "s1" });
  await api.createSchedule("w1", "0 0 2 * * *", {
    timezone: "Europe/Berlin",
    whenExpr: "{{ day_of_week }} != 0",
    stopExpr: "{{ done }}",
    catchup: true,
    catchupWindowSecs: 86400,
    catchupMaxRuns: 10,
  });
  assert.deepEqual(JSON.parse(last().body), {
    workflow_id: "w1",
    cron_expr: "0 0 2 * * *",
    enabled: true,
    timezone: "Europe/Berlin",
    when_expr: "{{ day_of_week }} != 0",
    stop_expr: "{{ done }}",
    catchup: true,
    catchup_window_secs: 86400,
    catchup_max_runs: 10,
  });
});

test("updateSchedule still patches only the given fields", async () => {
  const api = new Client(base, { token: "tok" });
  respond("PUT", "/api/schedules/s1", 200, { id: "s1" });
  await api.updateSchedule("s1", { timezone: "UTC" });
  assert.deepEqual(JSON.parse(last().body), { timezone: "UTC" });
});

// ── environments, settings, datasets ────────────────────────────────────────

test("environments are created, updated and deleted", async () => {
  const api = new Client(base, { token: "tok" });
  respond("POST", "/api/environments", 201, { id: "e1" });
  await api.createEnvironment("prod", { variables: { BUCKET: "s3://x" }, description: "live" });
  assert.deepEqual(JSON.parse(last().body), {
    name: "prod",
    description: "live",
    variables: { BUCKET: "s3://x" },
  });

  respond("PUT", "/api/environments/e1", 200, { id: "e1" });
  await api.updateEnvironment("e1", { variables: {} });
  // An explicit empty map is a real instruction ("no variables"), not an
  // omission, so it must survive to the wire.
  assert.deepEqual(JSON.parse(last().body), { variables: {} });

  respond("DELETE", "/api/environments/e1", 204, null);
  assert.equal(await api.deleteEnvironment("e1"), undefined);

  respond("GET", "/api/environments", 200, [{ id: "e1", secret_names: ["T"] }]);
  assert.deepEqual((await api.listEnvironments())[0].secret_names, ["T"]);
});

test("environment secrets are write-only", async () => {
  const api = new Client(base, { token: "tok" });
  respond("PUT", "/api/environments/e1/secrets/API_TOKEN", 204, null);
  await api.setEnvironmentSecret("e1", "API_TOKEN", "hunter2");
  assert.deepEqual(JSON.parse(last().body), { value: "hunter2" });

  respond("DELETE", "/api/environments/e1/secrets/API_TOKEN", 204, null);
  assert.equal(await api.deleteEnvironmentSecret("e1", "API_TOKEN"), undefined);
});

test("notification and dead-letter settings round-trip", async () => {
  const api = new Client(base, { token: "tok" });
  respond("GET", "/api/settings/notifications", 200, { slack_enabled: false });
  assert.equal((await api.getNotificationSettings()).slack_enabled, false);

  respond("PUT", "/api/settings/notifications", 200, { slack_enabled: true });
  await api.setNotificationSettings({ slack_enabled: true, slack_on: ["failed"] });
  assert.deepEqual(JSON.parse(last().body), { slack_enabled: true, slack_on: ["failed"] });

  respond("POST", "/api/settings/notifications/test", 200, { slack: "ok" });
  assert.equal((await api.testNotifications({ slack_enabled: true })).slack, "ok");

  respond("GET", "/api/settings/dead-letters", 200, { max_attempts: 3 });
  assert.equal((await api.getDeadLetterSettings()).max_attempts, 3);

  respond("PUT", "/api/settings/dead-letters", 200, { max_attempts: 5 });
  await api.setDeadLetterSettings(5);
  assert.deepEqual(JSON.parse(last().body), { max_attempts: 5 });
});

test("datasets and their lineage ledger", async () => {
  const api = new Client(base, { token: "tok" });
  respond("GET", "/api/datasets", 200, [{ uri: "s3://bucket/raw" }]);
  await api.listDatasets({ limit: 10 });
  assert.deepEqual(last().query, { limit: ["10"] });

  respond("GET", "/api/datasets/events", 200, [{ uri: "s3://bucket/raw" }]);
  await api.listDatasetEvents({ uri: "s3://bucket/raw" });
  assert.deepEqual(last().query, { uri: ["s3://bucket/raw"] });
});

// ── GitOps credentials ──────────────────────────────────────────────────────

test("a repository credential is set and cleared", async () => {
  const api = new Client(base, { token: "tok" });
  respond("PUT", "/api/git-repos/g1/auth", 200, { auth_kind: "https" });
  await api.setGitRepoAuth("g1", { kind: "https", username: "git", token: "ghp_x" });
  assert.deepEqual(JSON.parse(last().body), { kind: "https", username: "git", token: "ghp_x" });

  respond("DELETE", "/api/git-repos/g1/auth", 204, null);
  assert.equal(await api.clearGitRepoAuth("g1"), undefined);

  respond("POST", "/api/git-repos", 201, { id: "g1" });
  await api.connectGitRepo("https://example.com/x.git", { auth: { kind: "https", token: "t" } });
  assert.deepEqual(JSON.parse(last().body).auth, { kind: "https", token: "t" });
});

// ── artifacts ───────────────────────────────────────────────────────────────

test("artifacts move as bytes, not JSON", async () => {
  const api = new Client(base, { token: "tok" });
  const path = "/api/runs/r1/artifacts/extract/rows.csv";

  respond("PUT", path, 201, "local://r1/extract/rows.csv");
  assert.equal(
    await api.putArtifact("r1", "extract", "rows.csv", "a,b\n1,2\n"),
    "local://r1/extract/rows.csv",
  );
  assert.equal(last().body, "a,b\n1,2\n");
  assert.equal(last().headers["content-type"], "application/octet-stream");

  const png = new Uint8Array([0x89, 0x50, 0x4e, 0x47]);
  await api.putArtifact("r1", "extract", "rows.csv", png);
  assert.deepEqual(last().rawBody, Buffer.from(png));

  respond("GET", path, 200, png);
  assert.deepEqual(await api.getArtifact("r1", "extract", "rows.csv"), png);

  respond("GET", path + "/exists", 200, { exists: false });
  assert.equal(await api.artifactExists("r1", "extract", "rows.csv"), false);

  respond("POST", "/api/artifacts/sync", 200, { moved: 4 });
  assert.equal((await api.syncArtifacts()).moved, 4);
});

// ── observability ───────────────────────────────────────────────────────────

test("health, readiness, search and the timeseries", async () => {
  const api = new Client(base, { token: "tok" });
  respond("GET", "/api/health", 200, { db: "ok", active_runs: 2 });
  assert.equal((await api.health()).active_runs, 2);

  respond("GET", "/readyz", 200, "ready");
  assert.equal(await api.readyz(), "ready");

  respond("GET", "/api/search", 200, { query: "etl", runs: [] });
  await api.search("etl", { limit: 5 });
  assert.deepEqual(last().query, { q: ["etl"], limit: ["5"] });

  respond("GET", "/api/metrics/timeseries", 200, [{ day: "2026-01-01" }]);
  await api.metricsTimeseries({ days: 30, name: "etl" });
  assert.deepEqual(last().query, { days: ["30"], name: ["etl"] });

  respond("GET", "/api/approvals", 200, [{ run_id: "r1", task_name: "gate" }]);
  assert.equal((await api.listApprovals())[0].task_name, "gate");
});

// ── cross-SDK parity ────────────────────────────────────────────────────────

test("the client surface matches the Python SDK's, method for method", () => {
  // The two SDKs are documented as covering the same ground and are released
  // together, so a method added to one and forgotten in the other is a
  // documentation bug the moment it lands. This is the check that catches it.
  const pythonSource = path.join(import.meta.dirname, "..", "python", "dagron.py");
  if (!fs.existsSync(pythonSource)) return; // published tarball: no sibling SDK

  const src = fs.readFileSync(pythonSource, "utf8");
  // Only the class's own methods sit at exactly four spaces of indent — but the
  // slice still has to stop at the next top-level declaration. Today only
  // module-level helpers follow `Client`, so an unbounded slice happens to be
  // right; a class added after it would leak its methods in here and either
  // fail spuriously or mask a genuinely missing one.
  const anchor = "\nclass Client:";
  const start = src.indexOf(anchor);
  assert.ok(start !== -1, "`class Client:` not found in the Python SDK");
  const after = src.slice(start + anchor.length);
  const end = after.search(/^(?:class |def |@)/m);
  const body = end === -1 ? after : after.slice(0, end);
  assert.ok(!body.includes("\nclass "), "the Client slice must stop at the next class");
  const python = [...body.matchAll(/^ {4}def ([a-z][a-z0-9_]*)\(/gm)]
    .map((m) => m[1])
    .sort();

  const ts = Object.getOwnPropertyNames(Client.prototype)
    .filter((n) => n !== "constructor" && !n.startsWith("_"))
    .concat(["fromEnv"]) // a static, so not on the prototype
    .map((n) => n.replace(/[A-Z]/g, (c) => `_${c.toLowerCase()}`))
    .sort();

  assert.ok(python.length > 80, `expected the Python client's methods, found ${python.length}`);
  assert.deepEqual(ts, python);
});

test("breaking out of a stream releases it instead of hanging", { timeout: 5000 }, async () => {
  // `streamRun`/`streamEvents` delegate to one shared generator with `yield*`,
  // so a consumer's `break` has to propagate through the delegation and run the
  // `finally` that cancels the response body. If it did not, this test would
  // hang rather than fail.
  const api = new Client(base, { token: "tok" });
  sseBody = 'event: a\ndata: 1\n\nevent: b\ndata: 2\n\nevent: c\ndata: 3\n\n';
  const seen = [];
  for await (const ev of api.streamRun("r1")) {
    seen.push(ev.event);
    if (seen.length === 1) break;
  }
  assert.deepEqual(seen, ["a"]);
});

test("iterRuns refuses the pagination it drives itself", async () => {
  const api = new Client(base, { token: "tok" });
  for (const reserved of ["limit", "offset"]) {
    await assert.rejects(
      async () => {
        for await (const _ of api.iterRuns({ [reserved]: 10 })) break;
      },
      /iterRuns drives/,
    );
  }
});

test("waitRun outlives a shorter client timeout", { timeout: 15000 }, async () => {
  // The gateway holds the request for its whole wait budget. Without per-call
  // headroom the client's own timeout aborts just as the answer arrives — the
  // default 30s client timeout against the default 30s server wait.
  const slow = http.createServer((req, res) => {
    setTimeout(() => {
      res.writeHead(200, { "content-type": "application/json" });
      res.end('{"run_id":"r1","status":"succeeded","finished":true}');
    }, 1500);
  });
  await new Promise((r) => slow.listen(0, "127.0.0.1", r));
  const { address, port } = slow.address();
  try {
    const api = new Client(`http://${address}:${port}`, { token: "tok", timeout: 500 });
    const result = await api.waitRun("r1", { timeoutSecs: 120 });
    assert.equal(result.status, "succeeded");
    // The short client timeout still governs every other call.
    await assert.rejects(() => api.getRun("r1"), DagronError);
  } finally {
    await new Promise((r) => slow.close(r));
  }
});

/** Record the per-call transport budget waitRun asks _request for. */
async function waitRunBudget(clientTimeout, opts) {
  const api = new Client(base, { token: "tok", timeout: clientTimeout });
  let seen;
  const real = api._request.bind(api);
  api._request = (method, path, o) => {
    seen = o?.timeoutMs;
    return real(method, path, o);
  };
  respond("GET", "/api/runs/r1/wait", 200, { finished: true });
  await api.waitRun("r1", opts);
  return seen;
}

test("waitRun never shrinks a generous client timeout", async () => {
  // The headroom only ever widens: a caller who set a long client timeout keeps
  // it rather than being cut back to the server's budget plus a margin.
  assert.equal(await waitRunBudget(900000, undefined), 900000);
});

test("waitRun sizes its headroom off the server's clamped budget", async () => {
  // The server clamps timeout_secs to [1, 600]; sizing the transport off the
  // unclamped value would wait far past anything the gateway will honour.
  assert.equal(await waitRunBudget(1000, { timeoutSecs: 99999 }), 605000);
  // And the default budget is the server's 30s, not the client's timeout.
  assert.equal(await waitRunBudget(1000, undefined), 35000);
});

test("listGitRepos returns the registry object, not a bare list", async () => {
  // `GET /api/git-repos` answers repos plus the registry's own state: whether a
  // worker is running to sync them, and whether a credential can be stored.
  const api = new Client(base, { token: "tok" });
  respond("GET", "/api/git-repos", 200, {
    repos: [{ id: "g1" }],
    worker_online: true,
    credentials_configured: false,
  });
  const repos = await api.listGitRepos();
  assert.equal(repos.repos[0].id, "g1");
  assert.equal(repos.worker_online, true);
});
