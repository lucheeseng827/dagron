// New in 0.9: the Dag builder's full TaskSpec surface, in one run.
//
// Before 0.9 the TypeScript `Dag`/`task()` builder emitted four fields —
// `image`, `command`, `dependsOn`, `runnerClass` — so a DAG authored here
// could not set a retry, an env var, a sensor or a gate, never mind a
// sub-workflow trigger. @dagron/sdk now mirrors the Python client method for
// method, so this example is the TypeScript twin of
// examples/sdk/python/06_advanced_dag.py:
//
//   process-shard (fan-out) -> settle (sensor) -> sign-off (approval) -> chain-to-child (sub-workflow trigger)
//
//   node 02_advanced_dag.mjs
//
// Registers a tiny child workflow to chain to, resolves the approval gate
// programmatically, and cleans the child workflow up afterward — safe to
// re-run.

import { Client, Dag, DagronError } from "../../../sdks/typescript/index.mjs";

const API_URL = process.env.DAGRON_API_URL ?? "http://localhost:8080";
const EMAIL = process.env.DAGRON_EMAIL ?? "admin@local";
const PASSWORD = process.env.DAGRON_PASSWORD ?? "dagron-admin";

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
  const api = new Client(API_URL, { token: process.env.DAGRON_TOKEN });
  if (!api.token) await api.login(EMAIL, PASSWORD);

  // 1. A tiny child workflow for the parent DAG to chain to. `type: workflow`
  //    resolves its target by *registered name*, so this has to exist first.
  const child = new Dag("sdk-advanced-child-ts");
  child.task("child-step", { command: ["echo", "child ran"] });
  let createdChild = false;
  let childId;
  try {
    const wf = await api.createWorkflow(child, {
      name: "sdk-advanced-child-ts",
      description: "SDK example — chained child",
    });
    createdChild = true;
    childId = wf.id ?? wf.workflow_id;
  } catch (e) {
    if (!(e instanceof DagronError) || e.status !== 409) throw e;
    const existing = (await api.listWorkflows()).find((w) => w.name === "sdk-advanced-child-ts");
    childId = existing.id ?? existing.workflow_id;
    console.log("reusing existing child workflow:", childId);
  }

  try {
    // 2. The parent: fan-out -> sensor -> approval gate -> sub-workflow trigger.
    const dag = new Dag("sdk-advanced-dag-ts", { resultFrom: "chain-to-child" });

    // Fan-out: one task instance per item, named from `instanceKey`. A
    // downstream task that `dependsOn` the *base* name ("process-shard") fans
    // back in over every expanded copy.
    const fanout = dag.task("process-shard", {
      command: ["sh", "-c", "echo processing shard {{ item }}"],
      withItems: ["a", "b", "c"],
      instanceKey: "{{ item }}",
    });

    // A sensor holds no worker slot while it waits.
    const settle = dag.sensor("settle", { duration: "5s", dependsOn: [fanout] });

    // A human approval gate. Absent a decision within `timeoutSecs` it
    // resolves as `onTimeout` ("reject" by default) — a gate fails safe.
    const gate = dag.approval("sign-off", { dependsOn: [settle], timeoutSecs: 120 });

    // Sub-workflow trigger: submits the registered child by name and parks
    // until it is terminal, succeeding or failing with it.
    dag.trigger("chain-to-child", "sdk-advanced-child-ts", { dependsOn: [gate] });

    console.log("spec:", dag.toJSON());
    const runId = await api.submitRun(dag);
    console.log("submitted run:", runId);

    // 3. Wait for the gate to actually park, then approve it.
    let gateTask;
    for (let i = 0; i < 30; i++) {
      const run = await api.getRun(runId);
      const candidate = run.tasks.find((t) => t.name === "sign-off");
      if (candidate?.status === "awaiting_approval") {
        gateTask = candidate;
        break;
      }
      await sleep(1000);
    }
    if (!gateTask) throw new Error("'sign-off' never reached awaiting_approval within 30s");
    await api.approveTask(runId, gateTask.id);
    console.log("approved:", gateTask.id);

    // 4. Block for the rest — the sub-workflow trigger included.
    const result = await api.waitRun(runId, { timeoutSecs: 60 });
    console.log("run status:", result.status, "result:", result.result);
    if (result.failure) console.log("failure:", result.failure.message);
  } finally {
    if (createdChild && childId) {
      await api.deleteWorkflow(childId);
      console.log("cleaned up child workflow");
    }
  }

  // For reference — not submitted. `pool`/`gang` route to engine replicas or
  // co-scheduled groups that must already exist on your deployment.
  const reference = new Dag("sdk-scheduling-reference-ts");
  reference.task("distributed-step", {
    command: ["echo", "co-scheduled"],
    pool: "gpu-pool",
    priority: 10,
    gang: 4,
    // `cache`/`resources`/`gang`-as-object pass through verbatim (unlike the
    // top-level option names, their contents are the engine's own wire
    // schema, so keys stay snake_case here).
    cache: { key: "{{ params.day }}", ttl_secs: 3600 },
    produces: ["dataset://warehouse/daily_rollup"],
  });
  console.log("\nscheduling fields reference (not submitted):");
  console.log(reference.toJSON());
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
