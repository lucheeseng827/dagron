import { test } from "node:test";
import assert from "node:assert/strict";
import { Dag } from "./index.mjs";

test("builds a dagron spec with deps", () => {
  const dag = new Dag("etl");
  const a = dag.task("extract", { image: "alpine", command: ["echo", "hi"] });
  dag.task("load", { image: "alpine", command: ["true"], dependsOn: [a] });

  const spec = dag.toSpec();
  assert.equal(spec.name, "etl");
  assert.equal(spec.tasks.length, 2);
  assert.equal(spec.tasks[0].docker_image, "alpine");
  assert.deepEqual(spec.tasks[1].depends_on, ["extract"]);
  // Empty fields are omitted (clean spec).
  assert.ok(!("depends_on" in spec.tasks[0]));
});

test("toJSON is valid JSON (and thus valid dagron YAML input)", () => {
  const dag = new Dag("w");
  dag.task("t", { command: ["true"] });
  const parsed = JSON.parse(dag.toJSON());
  assert.equal(parsed.name, "w");
  assert.equal(parsed.tasks[0].name, "t");
});

test("rejects duplicate task names", () => {
  const dag = new Dag("w");
  dag.task("a", {});
  assert.throws(() => dag.task("a", {}), /duplicate task/);
});

test("rejects unknown dependency at build time", () => {
  const dag = new Dag("w");
  dag.task("a", { dependsOn: ["ghost"] });
  assert.throws(() => dag.toSpec(), /unknown task 'ghost'/);
});

test("rejects empty task names", () => {
  const dag = new Dag("w");
  assert.throws(() => dag.task("", {}), /requires a name/);
});

test("submit wraps the spec as {yaml} and returns run_id", async () => {
  const dag = new Dag("w");
  dag.task("t", { command: ["true"] });

  let captured;
  const realFetch = globalThis.fetch;
  globalThis.fetch = async (url, init) => {
    captured = { url, init };
    return new Response(JSON.stringify({ run_id: "run-123" }), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  };
  try {
    const runId = await dag.submit("http://localhost:8080/", { token: "tok" });
    assert.equal(runId, "run-123");
    assert.equal(captured.url, "http://localhost:8080/api/runs");
    assert.equal(captured.init.headers["authorization"], "Bearer tok");
    // Gateway contract: body is {"yaml": "<spec string>"}, not the raw spec.
    const body = JSON.parse(captured.init.body);
    assert.ok(typeof body.yaml === "string", "body must carry a `yaml` string");
    assert.equal(JSON.parse(body.yaml).name, "w");
  } finally {
    globalThis.fetch = realFetch;
  }
});

test("submit throws DagronError-shaped message on non-2xx", async () => {
  const dag = new Dag("w");
  dag.task("t", { command: ["true"] });

  const realFetch = globalThis.fetch;
  globalThis.fetch = async () =>
    new Response('{"error":"bad dag"}', { status: 400 });
  try {
    await assert.rejects(() => dag.submit("http://localhost:8080"), /dagron-api 400/);
  } finally {
    globalThis.fetch = realFetch;
  }
});
