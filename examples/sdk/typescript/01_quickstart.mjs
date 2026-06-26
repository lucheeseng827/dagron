// Quickstart for the dagron TypeScript/JavaScript SDK (@dagron/sdk).
//
// Builds a DAG in code, logs in to dagron-api, submits a run, and polls it to a
// terminal state. Zero dependencies — uses Node's global fetch (Node 18+).
//
//   node 01_quickstart.mjs
//
// Config via env (defaults match the local compose stack):
//   DAGRON_API_URL   default http://localhost:8080
//   DAGRON_TOKEN     session JWT (skips login if set)
//   DAGRON_EMAIL     default admin@local
//   DAGRON_PASSWORD  default dagron-admin

import { Dag } from "../../../sdks/typescript/index.mjs";

const API_URL = process.env.DAGRON_API_URL ?? "http://localhost:8080";
const EMAIL = process.env.DAGRON_EMAIL ?? "admin@local";
const PASSWORD = process.env.DAGRON_PASSWORD ?? "dagron-admin";

async function login(apiUrl, email, password) {
  const res = await fetch(`${apiUrl.replace(/\/$/, "")}/api/login`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ email, password }),
  });
  if (!res.ok) throw new Error(`login failed: ${res.status} ${await res.text()}`);
  return (await res.json()).token;
}

async function getRun(apiUrl, token, runId) {
  const res = await fetch(`${apiUrl.replace(/\/$/, "")}/api/runs/${runId}`, {
    headers: { authorization: `Bearer ${token}` },
  });
  if (!res.ok) throw new Error(`get_run failed: ${res.status} ${await res.text()}`);
  return res.json();
}

const TERMINAL = new Set(["succeeded", "failed", "cancelled"]);

async function main() {
  // 1. Author the DAG.
  const dag = new Dag("sdk-quickstart-ts");
  const extract = dag.task("extract", { command: ["echo", "extracted"] });
  const transform = dag.task("transform", {
    command: ["echo", "transformed"],
    dependsOn: [extract],
  });
  dag.task("load", { command: ["echo", "loaded"], dependsOn: [transform] });
  console.log("spec:", dag.toJSON());

  // 2. Auth (token from env, else login).
  const token = process.env.DAGRON_TOKEN ?? (await login(API_URL, EMAIL, PASSWORD));

  // 3. Submit — the SDK wraps the spec as {yaml} and returns the run id.
  const runId = await dag.submit(API_URL, { token });
  console.log("submitted run:", runId);

  // 4. Poll to a terminal state (the TS SDK is build+submit only; poll by hand).
  let run;
  for (let i = 0; i < 60; i++) {
    run = await getRun(API_URL, token, runId);
    if (TERMINAL.has(run.status)) break;
    await new Promise((r) => setTimeout(r, 1000));
  }
  if (!run || !TERMINAL.has(run.status)) {
    throw new Error(`run ${runId} did not reach a terminal state within 60s`);
  }
  console.log("run status:", run.status);
  for (const t of run.tasks ?? []) {
    console.log(`  - ${t.name.padEnd(10)} ${t.status}`);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
