// New in 0.9: a CI-shaped run — a revocable token instead of a password, a
// git commit-status check that says *what the run produced*, and the
// account-wide event feed instead of polling every list.
//
//   node 03_notify_and_automation.mjs
//
// TypeScript twin of examples/sdk/python/07_notify_and_automation.py. Three
// things a CI job wants that a human at the console does not:
//
// 1. `Client.fromEnv()` + `createToken()` — mint a token once, store it, and
//    no job ever holds the password that would mint another.
// 2. `notify.git.description` — a commit-status check that names what the
//    run produced, not just that it passed. Four `run.*` names resolve in it
//    besides the workflow's own parameters: `run.id`, `run.workflow`,
//    `run.status`, `run.images`.
// 3. `Client.streamEvents()` — one account-wide SSE connection instead of
//    polling `listRuns` on a timer.
//
// The commit-status update in step 2 is best-effort: without GITHUB_TOKEN (or
// GITLAB_TOKEN) configured on the server it is a documented no-op, not an
// error, so this example runs to completion either way. Set one on your
// dagron deployment to see the check actually land on a real commit.

import { Client, Dag, DagronError } from "../../../sdks/typescript/index.mjs";

const API_URL = process.env.DAGRON_API_URL ?? "http://localhost:8080";
const EMAIL = process.env.DAGRON_EMAIL ?? "admin@local";
const PASSWORD = process.env.DAGRON_PASSWORD ?? "dagron-admin";

async function main() {
  const api = new Client(API_URL, { token: process.env.DAGRON_TOKEN });
  if (!api.token) await api.login(EMAIL, PASSWORD);

  // 1. The token dance a CI job actually wants: mint once with a password
  //    session, then only ever read it back from the environment. Minting
  //    requires a password session — a token cannot mint another, which is
  //    what keeps a leaked one from outrunning revocation.
  //
  //    Which means: if DAGRON_TOKEN was already set, there is no password
  //    session to mint from and `createToken` would 403. That is not a problem
  //    to work around — it is the steady state this example is teaching.
  process.env.DAGRON_API_URL ??= api.baseUrl;
  if (process.env.DAGRON_TOKEN) {
    console.log("DAGRON_TOKEN already set — using it (minting needs a password session)");
  } else {
    const minted = await api.createToken("sdk-example-ci-ts", { expiresInDays: 1 });
    console.log("minted token:", minted.token.slice(0, 12) + "…", "(shown once, never again)");
    process.env.DAGRON_TOKEN = minted.token;
  }
  const ci = Client.fromEnv(); // what the job itself would call — no password in sight

  // 2. A DAG whose commit-status check reports what it built, not just
  //    pass/fail. `description` and the other git-notify fields are
  //    `{{ param }}`-templated, plus four `run.*` names the caller cannot
  //    supply itself. Nested `notify` content is the engine's own wire
  //    schema (snake_case), unlike the top-level task/Dag option names.
  const dag = new Dag("sdk-ci-build-ts", {
    parameters: { commit_sha: "0".repeat(40) },
    notify: {
      git: {
        provider: "github",
        repo: "your-org/your-repo",
        sha: "{{ commit_sha }}",
        context: "dagron/ci",
        description: "ran {{ run.workflow }} as {{ run.id }}",
      },
    },
  });
  dag.task("build", { command: ["echo", "build artifact"] });
  console.log("spec:", dag.toJSON());

  const runId = await ci.submitRun(dag, { parameters: { commit_sha: "a1b2c3d4e5f6" } });
  console.log("submitted run:", runId);

  // 3. The account-wide feed: one connection sees every run's task-state
  //    changes, which is what the console's live mode is built on. Filter to
  //    the run just submitted and stop as soon as it says something.
  try {
    // Milliseconds, not seconds — `streamEvents`/`streamRun` take a bare
    // `timeout` that goes straight to `setTimeout`, while every `timeoutSecs`
    // option in this file (and `waitRun` below) is seconds. The Python twin's
    // `stream_events(timeout=15)` is seconds because it feeds urllib, so the
    // same 15-second wait is written differently in each language. Passing 15
    // here would abort the feed in 15ms and always fall through to the
    // fallback below.
    for await (const ev of ci.streamEvents({ timeout: 15_000 })) {
      if (ev.data?.run_id === runId) {
        console.log("  event ->", ev.event, ev.data);
        break;
      }
    }
  } catch (e) {
    // An idle timeout aborts the underlying fetch, which the client wraps into
    // a DagronError like every other transport failure — there is no distinct
    // timeout type in this SDK. `status === 0` is as narrow as it gets from
    // out here: it separates that class from an HTTP error (a 403 on the feed
    // is not an idle feed and must not be swallowed as one). Telling a timer
    // abort from a genuine connection failure would need a discriminator the
    // SDK does not expose.
    if (e instanceof DagronError && e.status === 0) {
      console.log("  (feed idle — falling back to a direct wait)");
    } else {
      throw e;
    }
  }

  const result = await ci.waitRun(runId, { timeoutSecs: 30 });
  console.log("run status:", result.status, "result:", result.result);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
