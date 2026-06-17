// Starter workflows offered on the "New workflow" screen so a first-time user
// isn't faced with a blank editor. Each is a complete, runnable DAG spec; the
// last two demonstrate chaining one saved workflow from another via
// `workflow_ref` (see docs/WORKFLOW_UI_GUIDE.md).

export interface Starter {
  id: string;
  label: string;
  /// One-line description shown next to the option.
  description: string;
  /// The YAML spec loaded into the editor.
  spec: string;
}

export const STARTERS: Starter[] = [
  {
    id: "hello",
    label: "Hello world",
    description: "A single task that prints a message — the smallest runnable workflow.",
    spec: `name: hello-world
tasks:
  - name: greet
    command: ["sh", "-c", "echo hello from dagron"]
`,
  },
  {
    id: "sequence",
    label: "Two-step sequence",
    description: "prepare → process: the second task waits for the first.",
    spec: `name: my-workflow
tasks:
  - name: prepare
    command: ["sh", "-c", "echo prepare"]
  - name: process
    command: ["sh", "-c", "echo process"]
    depends_on: [prepare]
`,
  },
  {
    id: "diamond",
    label: "Diamond (fan-out / fan-in)",
    description: "a → {b, c} → d: b and c run in parallel, d waits for both.",
    spec: `name: diamond
tasks:
  - name: a
    command: ["sh", "-c", "echo a"]
  - name: b
    command: ["sh", "-c", "echo b"]
    depends_on: [a]
  - name: c
    command: ["sh", "-c", "echo c"]
    depends_on: [a]
  - name: d
    command: ["sh", "-c", "echo d"]
    depends_on: [b, c]
    max_attempts: 3
    retry_delay_secs: 1
`,
  },
  {
    id: "etl-child",
    label: "Reusable sub-workflow (etl)",
    description: "A build → process → publish pipeline meant to be chained from another workflow.",
    spec: `name: etl
tasks:
  - name: build
    command: ["sh", "-c", "echo build"]
  - name: process
    command: ["sh", "-c", "echo process"]
    depends_on: [build]
  - name: publish
    command: ["sh", "-c", "echo publish"]
    depends_on: [process]
`,
  },
  {
    id: "chained",
    label: "Chained workflows (calls etl)",
    description: "prepare → [runs the saved 'etl' workflow] → notify. Save the 'etl' starter first.",
    spec: `name: nightly
tasks:
  - name: prepare
    command: ["sh", "-c", "echo prepare"]
  # This step chains another SAVED workflow by name. Save the "etl" starter
  # first, then run this one — dagron inlines etl's tasks (build/process/publish)
  # right here when the run starts.
  - name: run-etl
    workflow_ref: etl
    depends_on: [prepare]
  - name: notify
    command: ["sh", "-c", "echo done"]
    depends_on: [run-etl]
`,
  },
];
