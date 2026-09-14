# @dagron/sdk

Author dagron workflows in TypeScript/JavaScript and drive the whole dagron
control plane — trigger runs, manage workflows and schedules, hold environments
and secrets, redrive dead letters, wire up GitOps — without writing REST calls by
hand.

- **Zero dependencies.** ESM, global `fetch`, Node 18+, hand-written `.d.ts`.
- **Two layers:** `Dag` (a validating spec builder) and `Client` (a typed wrapper
  over the authenticated `dagron-api` gateway).
- **Full coverage.** `0.9.0` covers the whole `dagron-api` HTTP surface and the
  engine's whole `TaskSpec`, method for method with the Python SDK. The version
  tracks the API version it speaks to, so `0.9.x` means "covers the 0.9 gateway".

## Install

```bash
npm install @dagron/sdk
```

## Author a DAG

```ts
import { Dag } from "@dagron/sdk";

const dag = new Dag("etl", {
  parameters: { day: "today" },
  tags: ["nightly"],
  resultFrom: "load",
});
const extract = dag.task("extract", { image: "alpine", command: ["echo", "{{ day }}"] });
dag.task("load", { image: "alpine", command: ["true"], dependsOn: [extract] });

console.log(dag.toJSON()); // valid dagron input (YAML is a JSON superset)
```

`task()` maps onto the engine's full `TaskSpec`: the basics (`image`, `command`,
`dependsOn`, `input`, `env`, `resources`, `serviceAccount`), the retry policy
(`maxAttempts`, `retryDelaySecs`, `retryMaxDelaySecs`, `retryOnTimeout`,
`retryBudgets`, `timeoutSecs`), flow control (`when`, `triggerRule`, `hook`,
`allowFailure`), fan-out (`withItems`, `withParam`, `instanceKey`), scheduling
(`runnerClass`, `pool`, `priority`, `gang`) and the rest (`cache`, `repeat`,
`produces`, `isolation`). The `Dag` constructor takes the spec-level block:
`parameters`, `tags`, `environment`, `taskDefaults`, `runTimeoutSecs`,
`maxActiveRuns`, `resultFrom`, `budget`, `deadline`, `notify`, `onDatasets`.

An unknown option throws rather than being silently dropped — a `retryDelay`
that quietly becomes no retry delay is the failure this SDK already refuses for
log filters.

Beyond leaf tasks, a task can be a **call** into a template, a **chain** into
another saved workflow, or one of the command-less kinds:

```ts
const dag = new Dag("release");
const build = dag.template("build", { parameters: { target: "release" } });
build.task("compile", { command: ["make", "{{ target }}"] });

dag.task("run-build", { template: "build", arguments: { target: "debug" } });
dag.approval("sign-off", { dependsOn: ["run-build"], timeoutSecs: 3600 });
dag.sensor("settle", { duration: "5m", dependsOn: ["sign-off"] });
dag.trigger("publish", "publish-artifacts", { dependsOn: ["settle"] });
```

`toSpec()`/`toJSON()` validate the graph client-side — unique names, known deps,
one kind per task, valid trigger rules and runner classes, resolvable template
calls, a `resultFrom` that names a real task, acyclicity — mirroring the server
so a bad DAG fails fast instead of costing a 400 round-trip.

## Drive the control plane

```ts
import { Client } from "@dagron/sdk";

const api = new Client("http://localhost:8080");
await api.login("admin@example.com", process.env.DAGRON_PASSWORD);

// For automation, mint a token once — this response is the only one that ever
// carries it — and put it in the CI job's environment, not a password.
const { token } = await api.createToken("ci", { expiresInDays: 90 });
// Then, in that job: Client.fromEnv() reads DAGRON_API_URL + DAGRON_TOKEN.
const ci = new Client("http://localhost:8080", { token });

// Trigger an ad-hoc run and block on it server-side.
const runId = await api.submitRun(dag, {
  parameters: { day: "2026-01-01" },
  idempotencyKey: "etl-2026-01-01",
});
const result = await api.waitRun(runId, { timeoutSecs: 600 });
console.log(result.status, result.result); // succeeded | failed | cancelled
if (result.failure) console.log(result.failure.message); // why, without a second call

// Save it as a reusable workflow and schedule it nightly in a real timezone.
const wf = await api.createWorkflow(dag, { description: "nightly ETL" });
await api.createSchedule(wf.id, "0 0 2 * * *", { timezone: "Europe/Berlin", catchup: true });

// Observe.
for await (const ev of api.streamEvents()) console.log(ev.event, ev.data);
```

Every method maps one `dagron-api` endpoint to one call and resolves to the
server's JSON. Non-2xx responses throw `DagronError`:

```ts
import { DagronError } from "@dagron/sdk";

try {
  await api.submitRun({ name: "x", tasks: [] });
} catch (e) {
  if (e instanceof DagronError) console.log(e.status, e.message); // 400 "…cycle"
}
```

### What `Client` covers

**Auth & identity** — `login` · `logout` · `me` · `fromEnv` · `createUser`
· `listUsers` · `listTokens` · `createToken` · `revokeToken`

**Runs** — `submitRun` · `listRuns` · `iterRuns` · `getRun` · `getRunSpec`
· `getRunGraph` · `getRunLogs` · `getTaskLogs` · `cancelRun` · `rerunRun`
· `resubmitRun` · `retryTask` · `clearTask` · `approveTask` · `rejectTask`
· `listApprovals` · `streamRun` · `streamEvents` · `waitRun` · `waitForRun`
· `setTriage` · `clearTriage` · `archiveRun` · `listArchivedRuns`
· `getArchivedRun`

**Workflows** — `listWorkflows` · `getWorkflow` · `createWorkflow`
· `updateWorkflow` · `deleteWorkflow` · `runWorkflow` · `listWorkflowRuns`
· `listWorkflowVersions` · `setWorkflowState` · `applyBundle` · `workflowBadge`
· `syncWorkflowToGit`

**Schedules & backfills** — `listSchedules` · `createSchedule` · `updateSchedule`
· `deleteSchedule` · `backfillSchedule` · `createBackfill` · `listBackfills`
· `getBackfill` · `cancelBackfill`

**Environments & settings** — `listEnvironments` · `createEnvironment`
· `updateEnvironment` · `deleteEnvironment` · `setEnvironmentSecret`
· `deleteEnvironmentSecret` · `getNotificationSettings`
· `setNotificationSettings` · `testNotifications` · `getDeadLetterSettings`
· `setDeadLetterSettings`

**Data & artifacts** — `listDatasets` · `listDatasetEvents` · `putArtifact`
· `getArtifact` · `artifactExists` · `syncArtifacts`

**Dead letters & GitOps** — `listDeadLetters` · `redriveDeadLetter`
· `discardDeadLetter` · `listGitRepos` · `connectGitRepo` · `setGitRepoAuth`
· `clearGitRepoAuth` · `syncGitRepo` · `disconnectGitRepo`

**Observability** — `metrics` · `metricsTimeseries` · `search` · `health`
· `healthz` · `readyz`

Not wrapped: `/api/audit`, `/api/fleet`, `/api/link*` and `/api/artifacts/rotate`
— absent, or answering a signpost, in this build.

## Test

```bash
node --test          # from sdks/typescript
```

The suite validates the builder and runs `Client` against an in-process fake
gateway on a real socket, so request construction is exercised end-to-end.
