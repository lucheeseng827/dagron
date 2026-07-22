import { test, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
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

function respond(method, path, status, payload) {
  responses.set(`${method} ${path}`, { status, payload });
}

function last() {
  return requests[requests.length - 1];
}

beforeEach(async () => {
  requests = [];
  responses = new Map();
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
      });

      if (url.pathname.endsWith("/stream")) {
        res.writeHead(200, { "content-type": "text/event-stream" });
        res.end(sseBody);
        return;
      }

      const hit = responses.get(`${req.method} ${url.pathname}`);
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
      if (typeof payload === "object") {
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
