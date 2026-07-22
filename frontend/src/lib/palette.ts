// Premade block palette shared by the spec editor (click-to-insert) and the
// visual DAG editor (click-to-insert or drag onto the canvas): each snippet is a
// well-formed, engine-supported piece of spec so users compose pipelines instead
// of typing YAML line by line. Insertion goes through the lossless spec-model
// round-trip, and clicked tasks auto-chain onto the current leaf tasks so each
// click visibly extends the pipeline; dragged tasks land where they are dropped
// — unchained on empty canvas, or spliced into a dependency edge.
//
// Every field emitted here is verified against the engine's TaskSpecInput /
// DagSpecInput (dagron-api routes/control.rs) — the palette never offers a knob
// the engine doesn't execute. There is deliberately no `env:` (OSS TaskSpecInput
// drops it), so block parameters (buckets, URLs, connection strings) live inline
// in the command where the task panel makes them obvious to edit.
//
// File-passing blocks use "${DAGRON_ARTIFACTS:-/tmp}": with the artifact store
// enabled (DAGRON_ARTIFACT_DIR, see docs/CONFIG.md) host-executor tasks in a run
// share that directory; containerized deployments should pass data through
// external storage instead — that's what the S3 blocks are for.

import { modelToYaml, parseModel, type Task, type WorkflowModel } from "@/lib/spec-model";

export type SnippetCategory =
  | "Tasks"
  | "Data & storage"
  | "Data engineering"
  | "ML & Python"
  | "Integration"
  | "Control flow"
  | "Run settings";

/// Display order for the palette rails.
export const SNIPPET_CATEGORIES: SnippetCategory[] = [
  "Tasks",
  "Data & storage",
  "Data engineering",
  "ML & Python",
  "Integration",
  "Control flow",
  "Run settings",
];

/// dataTransfer type used to drag a task block from the palette onto the DAG
/// canvas. The payload is the snippet id.
export const SNIPPET_MIME = "application/x-dagron-snippet";

interface SnippetBase {
  id: string;
  label: string;
  /// One-line "what you get" shown under the label.
  description: string;
  category: SnippetCategory;
}

/// Discriminated on `kind` so a task snippet can't exist without its name base
/// and builder, and a run snippet can't exist without its patch.
export type Snippet =
  | (SnippetBase & {
      kind: "task";
      /// Base for the generated unique task name.
      base: string;
      /// Build the task to append. The caller assigns a unique name (from
      /// `base`) and chooses depends_on (clicked blocks chain onto the current
      /// leaf tasks; dropped blocks start unchained). The chosen dependencies
      /// are passed in so a snippet can reference them (e.g. a `when` gate on
      /// the upstream task's output).
      makeTask: (dependsOn: string[]) => Omit<Task, "name" | "depends_on">;
    })
  | (SnippetBase & {
      kind: "run";
      /// Patch the model in place (top-level keys ride _extra). Returns an
      /// error message, or undefined on success.
      patchRun: (model: WorkflowModel) => string | undefined;
    });

// Shell fragment for the per-run shared dir (host executor + artifact store);
// falls back to /tmp so the block still runs anywhere.
const ART = '"${DAGRON_ARTIFACTS:-/tmp}"';

export const SNIPPETS: Snippet[] = [
  {
    id: "shell",
    label: "Shell task",
    description: "Run a command on the host executor.",
    category: "Tasks",
    kind: "task",
    base: "task",
    makeTask: () => ({ command: ["sh", "-c", "echo hello"] }),
  },
  {
    id: "docker",
    label: "Docker task",
    description: "Run the command inside a container image.",
    category: "Tasks",
    kind: "task",
    base: "docker-task",
    makeTask: () => ({
      command: ["sh", "-c", "echo hello from a container"],
      docker_image: "alpine:3.20",
    }),
  },
  {
    id: "retry",
    label: "Retrying task",
    description: "3 attempts, 10s delay, backoff capped at 5m.",
    category: "Tasks",
    kind: "task",
    base: "retry-task",
    makeTask: () => ({
      command: ["sh", "-c", "echo flaky step"],
      max_attempts: 3,
      retry_delay_secs: 10,
      _extra: { retry_max_delay_secs: 300 },
    }),
  },
  {
    id: "timeout",
    label: "Task with timeout",
    description: "Killed if it runs past 10 minutes.",
    category: "Tasks",
    kind: "task",
    base: "bounded-task",
    makeTask: () => ({
      command: ["sh", "-c", "echo long step"],
      timeout_secs: 600,
    }),
  },

  // ── Data & storage — S3 scratch space + intermediate compression ──────────
  {
    id: "s3-temp-storage",
    label: "S3: create temp storage",
    description: "Make a scratch prefix under s3://my-data-bucket/tmp/ — edit bucket + prefix.",
    category: "Data & storage",
    kind: "task",
    base: "s3-temp",
    makeTask: () => ({
      command: [
        "sh",
        "-c",
        "apk add --no-cache aws-cli >/dev/null && aws s3api put-object --bucket my-data-bucket --key tmp/my-pipeline/",
      ],
      docker_image: "alpine:3.20",
      timeout_secs: 300,
    }),
  },
  {
    id: "s3-download",
    label: "S3: download input",
    description: "Copy an object into the run's shared dir for downstream tasks.",
    category: "Data & storage",
    kind: "task",
    base: "s3-download",
    makeTask: () => ({
      command: [
        "sh",
        "-c",
        `apk add --no-cache aws-cli >/dev/null && aws s3 cp s3://my-data-bucket/input.csv ${ART}/input.csv`,
      ],
      docker_image: "alpine:3.20",
      timeout_secs: 300,
    }),
  },
  {
    id: "s3-upload",
    label: "S3: upload results",
    description: "Publish an output file from the shared dir back to S3.",
    category: "Data & storage",
    kind: "task",
    base: "s3-upload",
    makeTask: () => ({
      command: [
        "sh",
        "-c",
        `apk add --no-cache aws-cli >/dev/null && aws s3 cp ${ART}/output.csv s3://my-data-bucket/results/output.csv`,
      ],
      docker_image: "alpine:3.20",
      timeout_secs: 300,
    }),
  },
  {
    id: "s3-cleanup",
    label: "S3: delete temp storage",
    description: "Remove the scratch prefix when the run ends, pass or fail (all_done).",
    category: "Data & storage",
    kind: "task",
    base: "s3-cleanup",
    makeTask: () => ({
      command: [
        "sh",
        "-c",
        "apk add --no-cache aws-cli >/dev/null && aws s3 rm s3://my-data-bucket/tmp/my-pipeline/ --recursive",
      ],
      docker_image: "alpine:3.20",
      timeout_secs: 300,
      trigger_rule: "all_done",
    }),
  },
  {
    id: "compress",
    label: "Compress files (tar.gz)",
    description: "Bundle the shared dir into bundle.tar.gz — an intermediate compression step.",
    category: "Data & storage",
    kind: "task",
    base: "compress",
    makeTask: () => ({
      command: [
        "sh",
        "-c",
        `cd ${ART} && tar --exclude=bundle.tar.gz -czf bundle.tar.gz . && ls -lh bundle.tar.gz`,
      ],
      docker_image: "alpine:3.20",
    }),
  },
  {
    id: "extract",
    label: "Extract archive",
    description: "Unpack bundle.tar.gz in the shared dir for downstream tasks.",
    category: "Data & storage",
    kind: "task",
    base: "extract",
    makeTask: () => ({
      command: ["sh", "-c", `cd ${ART} && tar -xzf bundle.tar.gz && ls -lh`],
      docker_image: "alpine:3.20",
    }),
  },

  // ── Data engineering — everyday ETL steps ─────────────────────────────────
  {
    id: "csv-to-parquet",
    label: "CSV → Parquet",
    description: "Convert input.csv to output.parquet with pandas + pyarrow.",
    category: "Data engineering",
    kind: "task",
    base: "to-parquet",
    makeTask: () => ({
      command: [
        "sh",
        "-c",
        "pip install -q pandas pyarrow && python -c 'import os,pandas as pd; d=os.environ.get(\"DAGRON_ARTIFACTS\",\"/tmp\"); pd.read_csv(d+\"/input.csv\").to_parquet(d+\"/output.parquet\")'",
      ],
      docker_image: "python:3.12-slim",
      timeout_secs: 600,
    }),
  },
  {
    id: "pg-export",
    label: "Postgres → CSV export",
    description: "psql \\copy a query out to rows.csv — edit the connection URL + query.",
    category: "Data engineering",
    kind: "task",
    base: "pg-export",
    makeTask: () => ({
      command: [
        "sh",
        "-c",
        "psql postgresql://user:pass@db-host:5432/mydb -v ON_ERROR_STOP=1 -c \"\\\\copy (SELECT * FROM my_table) TO '${DAGRON_ARTIFACTS:-/tmp}/rows.csv' CSV HEADER\"",
      ],
      docker_image: "postgres:16-alpine",
      timeout_secs: 300,
    }),
  },
  {
    id: "dbt-run",
    label: "dbt run",
    description: "Run your dbt models — point at your project, or bake a custom image.",
    category: "Data engineering",
    kind: "task",
    base: "dbt-run",
    makeTask: () => ({
      command: [
        "sh",
        "-c",
        "pip install -q dbt-postgres && dbt run --project-dir /path/to/project --profiles-dir /path/to/profiles",
      ],
      docker_image: "python:3.12-slim",
      timeout_secs: 900,
    }),
  },
  {
    id: "quality-gate",
    label: "Data quality gate",
    description: "Fail the run unless input.csv has data rows — edit file + threshold.",
    category: "Data engineering",
    kind: "task",
    base: "quality-gate",
    makeTask: () => ({
      command: [
        "sh",
        "-c",
        'f="${DAGRON_ARTIFACTS:-/tmp}/input.csv"; rows=$(($(wc -l < "$f") - 1)); echo "$f: $rows data rows"; [ "$rows" -ge 1 ]',
      ],
    }),
  },

  // ── ML & Python — train / evaluate / score ────────────────────────────────
  {
    id: "python-script",
    label: "Python script",
    description: "Inline Python in a container — replace with your own code.",
    category: "ML & Python",
    kind: "task",
    base: "python",
    makeTask: () => ({
      command: ["python", "-c", "print('hello from dagron')"],
      docker_image: "python:3.12-slim",
    }),
  },
  {
    id: "train-model",
    label: "Train model (scikit-learn)",
    description: "Fit a demo classifier and save model.joblib to the shared dir.",
    category: "ML & Python",
    kind: "task",
    base: "train",
    makeTask: () => ({
      command: [
        "sh",
        "-c",
        "pip install -q scikit-learn joblib && python -c 'import os,joblib; from sklearn.datasets import load_iris; from sklearn.linear_model import LogisticRegression; X,y=load_iris(return_X_y=True); m=LogisticRegression(max_iter=200).fit(X,y); p=os.environ.get(\"DAGRON_ARTIFACTS\",\"/tmp\")+\"/model.joblib\"; joblib.dump(m,p); print(\"saved\",p)'",
      ],
      docker_image: "python:3.12-slim",
      timeout_secs: 900,
    }),
  },
  {
    id: "evaluate-model",
    label: "Evaluate model (gate)",
    description: "Score model.joblib; fail the pipeline below 0.9 accuracy.",
    category: "ML & Python",
    kind: "task",
    base: "evaluate",
    makeTask: () => ({
      command: [
        "sh",
        "-c",
        "pip install -q scikit-learn joblib && python -c 'import os,joblib; from sklearn.datasets import load_iris; X,y=load_iris(return_X_y=True); m=joblib.load(os.environ.get(\"DAGRON_ARTIFACTS\",\"/tmp\")+\"/model.joblib\"); s=m.score(X,y); print(\"accuracy\",round(s,4)); assert s>=0.9'",
      ],
      docker_image: "python:3.12-slim",
      timeout_secs: 900,
    }),
  },
  {
    id: "batch-inference",
    label: "Batch inference",
    description: "Load model.joblib, score a dataset, write predictions.csv.",
    category: "ML & Python",
    kind: "task",
    base: "predict",
    makeTask: () => ({
      command: [
        "sh",
        "-c",
        "pip install -q scikit-learn joblib && python -c 'import os,joblib; from sklearn.datasets import load_iris; d=os.environ.get(\"DAGRON_ARTIFACTS\",\"/tmp\"); X,_=load_iris(return_X_y=True); m=joblib.load(d+\"/model.joblib\"); open(d+\"/predictions.csv\",\"w\").write(\"\\n\".join(map(str,m.predict(X))))'",
      ],
      docker_image: "python:3.12-slim",
      timeout_secs: 900,
    }),
  },

  // ── Integration — the world outside the DAG ───────────────────────────────
  {
    id: "http-fetch",
    label: "HTTP fetch → file",
    description: "Pull an API payload into the shared dir; retries 3× on flaky networks.",
    category: "Integration",
    kind: "task",
    base: "fetch",
    makeTask: () => ({
      command: [
        "sh",
        "-c",
        'wget -qO "${DAGRON_ARTIFACTS:-/tmp}/response.json" https://api.example.com/data && wc -c "${DAGRON_ARTIFACTS:-/tmp}/response.json"',
      ],
      docker_image: "alpine:3.20",
      max_attempts: 3,
      retry_delay_secs: 5,
      timeout_secs: 120,
    }),
  },
  {
    id: "webhook-notify",
    label: "Notify webhook (Slack)",
    description: "POST a message to a Slack/Teams-style webhook — edit URL + text.",
    category: "Integration",
    kind: "task",
    base: "notify",
    makeTask: () => ({
      command: [
        "sh",
        "-c",
        "wget -qO- --header 'Content-Type: application/json' --post-data '{\"text\":\"dagron: my-pipeline finished\"}' https://hooks.slack.com/services/CHANGE-ME",
      ],
      docker_image: "alpine:3.20",
      timeout_secs: 60,
    }),
  },

  {
    id: "subworkflow",
    label: "Sub-workflow call",
    description: "Inline a saved workflow by name — edit the ref.",
    category: "Control flow",
    kind: "task",
    base: "run-workflow",
    makeTask: () => ({ command: [], workflow_ref: "my-saved-workflow" }),
  },
  {
    id: "cleanup",
    label: "Cleanup (always runs)",
    description: "Fires when upstream finishes, pass or fail (all_done).",
    category: "Control flow",
    kind: "task",
    base: "cleanup",
    makeTask: () => ({
      command: ["sh", "-c", "echo cleanup"],
      trigger_rule: "all_done",
    }),
  },
  {
    id: "on-failure",
    label: "Failure handler",
    description: "Fires only if an upstream task failed (one_failed).",
    category: "Control flow",
    kind: "task",
    base: "on-failure",
    makeTask: () => ({
      command: ["sh", "-c", "echo notify failure"],
      trigger_rule: "one_failed",
    }),
  },
  {
    id: "approval",
    label: "Approval gate",
    description: "Pauses for a human; rejects (fails safe) after 1h.",
    category: "Control flow",
    kind: "task",
    base: "approve",
    makeTask: () => ({
      command: [],
      _extra: { type: "approval", approval_timeout_secs: 3600, approval_on_timeout: "reject" },
    }),
  },
  {
    id: "run-timeout",
    label: "Auto-terminate run",
    description: "Cancel the whole run past a 1h wall-clock budget.",
    category: "Run settings",
    kind: "run",
    patchRun: (model) => {
      model._extra = { ...model._extra, run_timeout_secs: 3600 };
      return undefined;
    },
  },
  {
    id: "result-from",
    label: "Result from task",
    description: "The named task's output becomes the run's result.",
    category: "Run settings",
    kind: "run",
    patchRun: (model) => {
      const last = model.tasks[model.tasks.length - 1];
      if (!last) return "add a task first — result_from must name one";
      model._extra = { ...model._extra, result_from: last.name };
      return undefined;
    },
  },
  {
    id: "notify-slack",
    label: "Slack on failure",
    description: "Post to a Slack webhook when the run fails or misses its SLA.",
    category: "Run settings",
    kind: "run",
    patchRun: (model) => {
      const prev = (model._extra?.notify as Record<string, unknown> | undefined) ?? {};
      model._extra = {
        ...model._extra,
        notify: { ...prev, slack: { webhook_url: "{{ slack_webhook }}" } },
      };
      return undefined;
    },
  },
  {
    id: "notify-webhook",
    label: "Webhook on completion",
    description: "POST a JSON event to an HTTP endpoint on every run outcome.",
    category: "Run settings",
    kind: "run",
    patchRun: (model) => {
      const prev = (model._extra?.notify as Record<string, unknown> | undefined) ?? {};
      model._extra = {
        ...model._extra,
        notify: { ...prev, webhook: { url: "https://example.com/hooks/dagron" } },
      };
      return undefined;
    },
  },
  {
    id: "environment",
    label: "Use environment",
    description: "Run against a named environment: {{ env.NAME }} vars + its secrets.",
    category: "Run settings",
    kind: "run",
    patchRun: (model) => {
      model._extra = { ...model._extra, environment: "staging" };
      return undefined;
    },
  },
  {
    id: "task-defaults",
    label: "Task defaults",
    description: "Retries and timeout declared once, applied to every task.",
    category: "Run settings",
    kind: "run",
    patchRun: (model) => {
      model._extra = {
        ...model._extra,
        task_defaults: { max_attempts: 3, retry_delay_secs: 10, timeout_secs: 300 },
      };
      return undefined;
    },
  },
  {
    id: "branch-on-output",
    label: "Branch on output",
    description: "Runs only if the previous task printed the expected value.",
    category: "Control flow",
    kind: "task",
    base: "deploy",
    makeTask: (dependsOn) => ({
      command: ["sh", "-c", "echo deploying"],
      // The gate must reference a dependency — bind it to the task this block
      // chains onto. A drag-drop (no deps yet) gets a placeholder to edit.
      _extra: { when: `{{ tasks.${dependsOn[0] ?? "check"}.output }} == go` },
    }),
  },
  {
    id: "repeat-until",
    label: "Repeat until",
    description: "Poll-until-done: re-runs (30×, 10s apart) until output == done.",
    category: "Control flow",
    kind: "task",
    base: "poll",
    makeTask: () => ({
      command: ["sh", "-c", "check-status"],
      _extra: { repeat: { until: "{{ output }} == done", max_iterations: 30, delay_secs: 10 } },
    }),
  },
];

/// Look up a snippet by id (used to resolve a drag payload on drop).
export function snippetById(id: string): Snippet | undefined {
  return SNIPPETS.find((s) => s.id === id);
}

/// Generate a unique task name from a snippet's base ("cleanup", "cleanup-2", …).
function uniqueName(base: string, tasks: Task[]): string {
  const names = new Set(tasks.map((t) => t.name));
  if (!names.has(base)) return base;
  for (let i = 2; ; i++) {
    const n = `${base}-${i}`;
    if (!names.has(n)) return n;
  }
}

/// Leaf tasks (nothing depends on them) — where an appended step chains on.
export function leafNames(tasks: Task[]): string[] {
  const depended = new Set(tasks.flatMap((t) => t.depends_on));
  return tasks.filter((t) => !depended.has(t.name)).map((t) => t.name);
}

/// Materialize a task snippet as a concrete, uniquely-named task. `dependsOn`
/// is the caller's chaining policy: leaf tasks for click-to-append, empty for a
/// drag-drop (the user wires dependencies by hand).
export function buildPaletteTask(
  snippet: Extract<Snippet, { kind: "task" }>,
  tasks: Task[],
  dependsOn: string[],
): Task {
  return { depends_on: dependsOn, ...snippet.makeTask(dependsOn), name: uniqueName(snippet.base, tasks) };
}

/// Apply a palette snippet to the current spec text. A blank editor is
/// scaffolded into a fresh workflow; otherwise the spec must parse (the caller
/// disables the palette when it doesn't, so an error here is a race, not UX).
export function applySnippet(spec: string, snippet: Snippet): { spec?: string; error?: string } {
  let model: WorkflowModel;
  if (spec.trim() === "") {
    model = { name: "my-pipeline", tasks: [] };
  } else {
    const parsed = parseModel(spec);
    if (!parsed.model) return { error: parsed.error ?? "spec does not parse" };
    model = parsed.model;
  }

  if (snippet.kind === "task") {
    model.tasks = [...model.tasks, buildPaletteTask(snippet, model.tasks, leafNames(model.tasks))];
  } else {
    const err = snippet.patchRun(model);
    if (err) return { error: err };
  }
  return { spec: modelToYaml(model) };
}
