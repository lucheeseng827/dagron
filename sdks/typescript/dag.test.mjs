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
  dag.task("a", { command: ["true"], dependsOn: ["ghost"] });
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


// ── spec-level properties ────────────────────────────────────────────────────

test("emits every spec-level field that was set", () => {
  const dag = new Dag("etl", {
    runnerClass: "etl",
    parameters: { day: "today" },
    tags: ["nightly"],
    environment: "prod",
    taskDefaults: { max_attempts: 3 },
    runTimeoutSecs: 3600,
    maxActiveRuns: 1,
    resultFrom: "load",
    budget: { tasks: 50 },
    deadline: { within: "2h" },
    notify: { slack: { webhook_url: "https://hooks.example/x", on: ["failed"] } },
    onDatasets: ["s3://bucket/raw"],
    datasetsMode: "all",
  });
  dag.task("load", { command: ["true"] });
  const spec = dag.toSpec();
  assert.deepEqual(spec.parameters, { day: "today" });
  assert.deepEqual(spec.tags, ["nightly"]);
  assert.equal(spec.environment, "prod");
  assert.deepEqual(spec.task_defaults, { max_attempts: 3 });
  assert.equal(spec.run_timeout_secs, 3600);
  assert.equal(spec.max_active_runs, 1);
  assert.equal(spec.result_from, "load");
  assert.deepEqual(spec.budget, { tasks: 50 });
  assert.deepEqual(spec.deadline, { within: "2h" });
  assert.deepEqual(spec.on_datasets, ["s3://bucket/raw"]);
  assert.equal(spec.datasets_mode, "all");
});

test("omits spec-level fields that were not set", () => {
  const dag = new Dag("w");
  dag.task("a", { command: ["true"] });
  assert.deepEqual(Object.keys(dag.toSpec()).sort(), ["name", "tasks"]);
});

test("resultFrom must name a real task", () => {
  const dag = new Dag("w", { resultFrom: "ghost" });
  dag.task("a", { command: ["true"] });
  assert.throws(() => dag.toSpec(), /names no task/);
});

test("runTimeoutSecs of 0 is rejected", () => {
  const dag = new Dag("w", { runTimeoutSecs: 0 });
  dag.task("a", { command: ["true"] });
  assert.throws(() => dag.toSpec(), /expected >= 1/);
});

test("runner class charset is enforced, but a template is left to the server", () => {
  for (const bad of ["ETL", "with space", "other", "x".repeat(65)]) {
    const dag = new Dag("w", { runnerClass: bad });
    dag.task("a", { command: ["true"] });
    assert.throws(() => dag.toSpec(), /runner_class/, bad);
  }
  // `{{ pool }}` is only a real class name after the server substitutes it.
  const ok = new Dag("w", { runnerClass: "{{ pool }}" });
  ok.task("a", { command: ["true"], runnerClass: "{{ pool }}" });
  assert.equal(ok.toSpec().runner_class, "{{ pool }}");
});

test("an unknown option throws instead of being silently dropped", () => {
  assert.throws(() => new Dag("w", { runnerClas: "etl" }), /unknown Dag option/);
  const dag = new Dag("w");
  assert.throws(() => dag.task("a", { retryDelay: 5 }), /unknown task option/);
});

test("toSpec does not expose internal task state", () => {
  const dag = new Dag("w");
  dag.task("a", { command: ["true"] });
  dag.toSpec().tasks[0].command.push("mutated");
  assert.deepEqual(dag.toSpec().tasks[0].command, ["true"]);
});

test("rejects a dependency cycle", () => {
  const dag = new Dag("w");
  dag.task("a", { command: ["true"], dependsOn: ["b"] });
  dag.task("b", { command: ["true"], dependsOn: ["a"] });
  assert.throws(() => dag.toSpec(), /contains a cycle/);
});

// ── task kinds ───────────────────────────────────────────────────────────────

test("approval gate", () => {
  const dag = new Dag("w");
  dag.task("build", { command: ["true"] });
  dag.approval("gate", { dependsOn: ["build"], timeoutSecs: 3600, onTimeout: "reject" });
  const t = dag.toSpec().tasks[1];
  assert.equal(t.type, "approval");
  assert.equal(t.approval_timeout_secs, 3600);
  assert.equal(t.approval_on_timeout, "reject");
  assert.ok(!("command" in t));
});

test("rejects a bad approval timeout resolution", () => {
  const dag = new Dag("w");
  assert.throws(() => dag.approval("gate", { onTimeout: "maybe" }), /approve.*reject/);
});

test("sensor forms map onto the wire's wait block", () => {
  const dag = new Dag("w");
  dag.sensor("wait_5m", { duration: "5m" });
  dag.sensor("wait_data", { dataset: "s3://bucket/raw" });
  const tasks = dag.toSpec().tasks;
  assert.equal(tasks[0].type, "wait");
  assert.deepEqual(tasks[0].wait, { for: "5m" });
  assert.deepEqual(tasks[1].wait, { dataset: "s3://bucket/raw" });
});

test("a sensor needs exactly one form", () => {
  const none = new Dag("w");
  none.sensor("none", {});
  assert.throws(() => none.toSpec(), /exactly one of/);
  const both = new Dag("w");
  both.sensor("both", { duration: "5m", until: "2026-01-01T00:00:00Z" });
  assert.throws(() => both.toSpec(), /exactly one of/);
});

test("a wait block only belongs on a wait task, and never on a hook", () => {
  const stray = new Dag("w");
  stray.task("a", { command: ["true"], wait: { duration: "5m" } });
  assert.throws(() => stray.toSpec(), /not `type: wait`/);
  const hooked = new Dag("w");
  hooked.sensor("settle", { duration: "5m", hook: "on_exit" });
  assert.throws(() => hooked.toSpec(), /wait sensor and a hook/);
});

test("sub-workflow trigger", () => {
  const dag = new Dag("w");
  dag.trigger("child", "downstream", { arguments: { shard: "1" } });
  const t = dag.toSpec().tasks[0];
  assert.equal(t.type, "workflow");
  assert.equal(t.workflow, "downstream");
  assert.deepEqual(t.arguments, { shard: "1" });
});

test("a workflow target belongs only to a workflow task, and is required on one", () => {
  const stray = new Dag("w");
  stray.task("a", { command: ["true"], workflow: "other" });
  assert.throws(() => stray.toSpec(), /not `type: workflow`/);
  const empty = new Dag("w");
  empty.task("a", { taskType: "workflow" });
  assert.throws(() => empty.toSpec(), /names no `workflow:`/);
});

test("a command-less kind cannot carry a command", () => {
  const dag = new Dag("w");
  dag.task("gate", { taskType: "approval", command: ["true"] });
  assert.throws(() => dag.toSpec(), /command-less kind/);
});

test("a leaf is exactly one kind", () => {
  const none = new Dag("w");
  none.task("a", {});
  assert.throws(() => none.toSpec(), /needs a `command`/);
  const two = new Dag("w");
  two.task("a", { command: ["true"], workflowRef: "other" });
  assert.throws(() => two.toSpec(), /more than one of/);
});

test("arguments need a callee", () => {
  const dag = new Dag("w");
  dag.task("a", { command: ["true"], arguments: { k: "v" } });
  assert.throws(() => dag.toSpec(), /no `template` or `type: workflow`/);
});

test("dependsOn may forward-reference into a chain", () => {
  // `call.inner` only exists once the chain is inlined server-side, so the
  // builder must defer rather than call it an unknown dependency.
  const dag = new Dag("w");
  dag.task("call", { workflowRef: "child" });
  dag.task("after", { command: ["true"], dependsOn: ["call.inner"] });
  assert.equal(dag.toSpec().tasks.length, 2);
});

// ── templates ────────────────────────────────────────────────────────────────

test("declares and calls a template", () => {
  const dag = new Dag("w");
  const tpl = dag.template("build", { parameters: { target: "release" } });
  tpl.task("compile", { command: ["make", "{{ target }}"] });
  dag.task("run-build", { template: "build", arguments: { target: "debug" } });

  const spec = dag.toSpec();
  assert.equal(spec.templates[0].name, "build");
  assert.deepEqual(spec.templates[0].parameters, { target: "release" });
  assert.equal(spec.templates[0].tasks[0].name, "compile");
  assert.equal(spec.tasks[0].template, "build");
  assert.deepEqual(spec.tasks[0].arguments, { target: "debug" });
});

test("rejects a call to an undeclared template, and a duplicate declaration", () => {
  const dag = new Dag("w");
  dag.task("call", { template: "ghost" });
  assert.throws(() => dag.toSpec(), /unknown template 'ghost'/);
  const dup = new Dag("w");
  dup.template("t");
  assert.throws(() => dup.template("t"), /duplicate template/);
});

test("a template's sub-graph is validated too", () => {
  const dag = new Dag("w");
  const tpl = dag.template("t");
  tpl.task("a", { command: ["true"], dependsOn: ["ghost"] });
  dag.task("call", { template: "t" });
  assert.throws(() => dag.toSpec(), /unknown task 'ghost'/);
});

// ── task options ─────────────────────────────────────────────────────────────

test("scheduling, retry and data options reach the spec", () => {
  const dag = new Dag("w");
  dag.task("train", {
    command: ["train.sh"],
    pool: "gpu",
    priority: 10,
    runnerClass: "ml_training",
    retryMaxDelaySecs: 300,
    retryOnTimeout: false,
    retryBudgets: { "gpu-ecc": 8, "nan-loss": 0 },
    gang: 4,
    produces: ["s3://bucket/model"],
    cache: { key: "{{ params.day }}", max_age_secs: 86400 },
    isolation: { read_only_root: true },
  });
  const t = dag.toSpec().tasks[0];
  assert.equal(t.pool, "gpu");
  assert.equal(t.priority, 10);
  assert.equal(t.retry_max_delay_secs, 300);
  assert.equal(t.retry_on_timeout, false);
  assert.deepEqual(t.retry_budgets, { "gpu-ecc": 8, "nan-loss": 0 });
  assert.deepEqual(t.gang, { size: 4 });
  assert.deepEqual(t.produces, ["s3://bucket/model"]);
  assert.deepEqual(t.cache, { key: "{{ params.day }}", max_age_secs: 86400 });
  assert.deepEqual(t.isolation, { read_only_root: true });
});

test("a zero priority is omitted (it means: fall back to task_defaults)", () => {
  const dag = new Dag("w");
  dag.task("a", { command: ["true"], priority: 0 });
  assert.ok(!("priority" in dag.toSpec().tasks[0]));
});

test("fan-out and flow-control options", () => {
  const dag = new Dag("w");
  dag.task("a", { command: ["true"] });
  dag.task("shard", {
    command: ["run", "{{ item }}"],
    withItems: ["eu", "us"],
    instanceKey: "{{ item }}",
  });
  dag.task("cleanup", { command: ["true"], hook: "on_exit", allowFailure: true });
  const tasks = dag.toSpec().tasks;
  assert.deepEqual(tasks[1].with_items, ["eu", "us"]);
  assert.equal(tasks[1].instance_key, "{{ item }}");
  assert.equal(tasks[2].hook, "on_exit");
  assert.equal(tasks[2].allow_failure, true);
});

test("rejects an unknown trigger rule and an unknown hook", () => {
  const dag = new Dag("w");
  dag.task("a", { command: ["true"], triggerRule: "sometimes" });
  assert.throws(() => dag.toSpec(), /invalid trigger_rule/);
  const hooked = new Dag("w");
  assert.throws(() => hooked.task("a", { command: ["true"], hook: "on_tuesday" }), /hook must be/);
});

test("a when: must depend on the task whose output it reads", () => {
  const bad = new Dag("w");
  bad.task("a", { command: ["true"] });
  bad.task("b", { command: ["true"], when: "{{ tasks.a.output }} == go" });
  assert.throws(() => bad.toSpec(), /add it to dependsOn/);
  const good = new Dag("w");
  good.task("a", { command: ["true"] });
  good.task("b", { command: ["true"], dependsOn: ["a"], when: "{{ tasks.a.output }} == go" });
  assert.equal(good.toSpec().tasks[1].when, "{{ tasks.a.output }} == go");
});

test("repeat is validated the way the engine validates it", () => {
  const empty = new Dag("w");
  empty.task("poll", { command: ["check"], repeat: { until: "", max_iterations: 5 } });
  assert.throws(() => empty.toSpec(), /repeat.until is empty/);
  const zero = new Dag("w");
  zero.task("poll", { command: ["check"], repeat: { until: "done", max_iterations: 0 } });
  assert.throws(() => zero.toSpec(), /max_iterations must be >= 1/);
  const gate = new Dag("w");
  gate.approval("gate");
  gate._tasks[0].repeat = { until: "done", max_iterations: 3 };
  assert.throws(() => gate.toSpec(), /cannot combine `repeat`/);
});

test("env accepts a map, entries, and a secret reference", () => {
  const dag = new Dag("w");
  dag.task("a", { command: ["true"], env: { FOO: "bar" } });
  dag.task("b", {
    command: ["true"],
    env: [{ name: "TOKEN", valueFrom: { secret: "API_TOKEN" } }, { name: "X", value: "1" }],
  });
  const tasks = dag.toSpec().tasks;
  assert.deepEqual(tasks[0].env, [{ name: "FOO", value: "bar" }]);
  assert.deepEqual(tasks[1].env, [
    { name: "TOKEN", value_from: { secret: "API_TOKEN" } },
    { name: "X", value: "1" },
  ]);
});

test("env rejects an entry with neither value nor valueFrom", () => {
  const dag = new Dag("w");
  assert.throws(() => dag.task("a", { command: ["true"], env: [{ name: "X" }] }), /value/);
});

test("repeat rejects a key the wire does not have", () => {
  // A camelCased `maxIterations` would leave the required field unset: local
  // validation would pass and the submit would be refused.
  const dag = new Dag("w");
  assert.throws(
    () => dag.task("poll", { command: ["check"], repeat: { until: "done", maxIterations: 3 } }),
    /unknown repeat option/,
  );
});

test("Template#toSpec does not expose internal task state", () => {
  // Read directly (not through Dag#toSpec, whose outer clone would mask it),
  // the returned tasks must not be the builder's own list.
  const dag = new Dag("w");
  const tpl = dag.template("t");
  tpl.task("a", { command: ["true"] });
  tpl.toSpec().tasks[0].command.push("mutated");
  assert.deepEqual(tpl.toSpec().tasks[0].command, ["true"]);
});
