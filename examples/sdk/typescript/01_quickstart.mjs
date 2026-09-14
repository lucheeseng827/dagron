// Quickstart for the dagron TypeScript/JavaScript SDK (@dagron/sdk).
//
// Builds a DAG in code, authenticates to dagron-api, submits a run, and blocks
// on it — all through the SDK. Zero dependencies (Node's global fetch, Node 18+).
//
//   node 01_quickstart.mjs
//
// Config via env (defaults match the local compose stack):
//   DAGRON_API_URL   default http://localhost:8080
//   DAGRON_TOKEN     access token or session JWT (skips login if set)
//   DAGRON_EMAIL     default admin@local
//   DAGRON_PASSWORD  default dagron-admin

import { Client, Dag } from "../../../sdks/typescript/index.mjs";

const API_URL = process.env.DAGRON_API_URL ?? "http://localhost:8080";
const EMAIL = process.env.DAGRON_EMAIL ?? "admin@local";
const PASSWORD = process.env.DAGRON_PASSWORD ?? "dagron-admin";

async function main() {
  // 1. Author the DAG. `resultFrom` names the task whose output becomes the
  //    run's result, which is what `waitRun` hands back.
  const dag = new Dag("sdk-quickstart-ts", { resultFrom: "load" });
  const extract = dag.task("extract", { command: ["echo", "extracted"] });
  const transform = dag.task("transform", {
    command: ["echo", "transformed"],
    dependsOn: [extract],
  });
  dag.task("load", { command: ["echo", "loaded"], dependsOn: [transform] });
  console.log("spec:", dag.toJSON());

  // 2. Auth: a token from the environment, else a password login. In CI, prefer
  //    a token — `createToken` mints one and it can be revoked on its own.
  const api = new Client(API_URL, { token: process.env.DAGRON_TOKEN });
  if (!api.token) await api.login(EMAIL, PASSWORD);

  // 3. Submit — the SDK wraps the spec as {yaml} and returns the run id.
  const runId = await api.submitRun(dag);
  console.log("submitted run:", runId);

  // 4. Block on it server-side: one long-poll on the engine's own event feed,
  //    rather than a poll loop with an interval to guess at. A wait that times
  //    out comes back `finished: false`, so we just ask again.
  let result;
  for (let i = 0; i < 6; i++) {
    result = await api.waitRun(runId, { timeoutSecs: 10 });
    if (result.finished) break;
  }
  if (!result?.finished) {
    throw new Error(`run ${runId} did not reach a terminal state within 60s`);
  }
  console.log("run status:", result.status, "result:", result.result);
  // `failure` explains a failed run without a second round trip.
  if (result.failure) console.log("failure:", result.failure.message);

  // 5. The per-task rows, for the breakdown.
  const run = await api.getRun(runId);
  for (const t of run.tasks ?? []) {
    console.log(`  - ${t.name.padEnd(10)} ${t.status}`);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
